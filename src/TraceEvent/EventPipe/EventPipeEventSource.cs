﻿using FastSerialization;
using Microsoft.Diagnostics.Tracing.EventPipe;
using Microsoft.Diagnostics.Tracing.Parsers;
using Microsoft.Diagnostics.Tracing.Parsers.Clr;
using Microsoft.Diagnostics.Tracing.Session;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Microsoft.Diagnostics.Tracing
{
    unsafe public class EventPipeEventSourceNew : TraceEventDispatcher, IFastSerializable
    {
        public EventPipeEventSourceNew(string fileName)
        {
            // TODO need to get real values for this as well as the process ID and name.  
            _processId = 0xFFFE;    // Arbitrary
            _processName = "ProcessBeingTraced";
            osVersion = new Version("0.0.0.0");
            cpuSpeedMHz = 10;
            pointerSize = 8; // V1 EventPipe only supports Linux which is x64 only.
            numberOfProcessors = 1;

            _deserializer = new Deserializer(new PinnedStreamReader(fileName, 0x20000), fileName);
            _deserializer.RegisterFactory("Microsoft.DotNet.Runtime.EventPipeFile", delegate { return this; });
        
            var entryObj = _deserializer.GetEntryObject();
            // Because we told the deserialize to use 'this' when creating a EventPipeFile, we 
            // expect the entry object to be 'this'.
            Debug.Assert(entryObj == this);

            _eventParser = new EventPipeTraceEventParser(this);
        }

        #region private
        // I put these in the private section because they are overrides, and thus don't ADD to the API.  
        public override int EventsLost => 0;
        public override bool Process()
        {
            PinnedStreamReader deserializerReader = (PinnedStreamReader)_deserializer.Reader;

            deserializerReader.Goto(_startEventOfStream);
            while (deserializerReader.Current < _endOfEventStream)
            {
                TraceEventNativeMethods.EVENT_RECORD* eventRecord = ReadEvent(deserializerReader);
                if (eventRecord != null)
                {
                    // in the code below we set sessionEndTimeQPC to be the timestamp of the last event.  
                    // Thus the new timestamp should be later, and not more than 1 day later.  
                    Debug.Assert(sessionEndTimeQPC <= eventRecord->EventHeader.TimeStamp);
                    Debug.Assert(sessionEndTimeQPC == 0 || eventRecord->EventHeader.TimeStamp - sessionEndTimeQPC < _QPCFreq * 24 * 3600);

                    TraceEvent event_ = Lookup(eventRecord);
                    Dispatch(event_);
                    sessionEndTimeQPC = eventRecord->EventHeader.TimeStamp;
                }
            }

            return true;
        }

        internal override string ProcessName(int processID, long timeQPC)
        {
            return _processName;
        }

        private TraceEventNativeMethods.EVENT_RECORD* ReadEvent(PinnedStreamReader reader)
        {
            // Guess that the event is < 1000 bytes or whatever is left in the stream.  
            int eventSizeGuess = Math.Min(1000, _endOfEventStream.Sub(reader.Current));
            EventPipeEventHeader* eventData = (EventPipeEventHeader*)reader.GetPointer(eventSizeGuess);
            // Basic sanity checks.  Are the timestamps and sizes sane.  
            Debug.Assert(sessionEndTimeQPC <= eventData->TimeStamp);
            Debug.Assert(sessionEndTimeQPC == 0 || eventData->TimeStamp - sessionEndTimeQPC < _QPCFreq * 24 * 3600);
            Debug.Assert(0 <= eventData->PayloadSize && eventData->PayloadSize <= eventData->EventSize);
            Debug.Assert(eventData->MetaDataId <= reader.Current);       // IDs are the location in the of of the data, so it comes before
            Debug.Assert(0 < eventData->EventSize && eventData->EventSize < 0x20000);  // TODO really should be 64K but BulkSurvivingObjectRanges needs fixing.

            if (eventSizeGuess < eventData->EventSize)
                eventData = (EventPipeEventHeader*)reader.GetPointer(eventData->EventSize);

            Debug.Assert(0 <= EventPipeEventHeader.StackBytesSize(eventData) && EventPipeEventHeader.StackBytesSize(eventData) <= eventData->EventSize);

            // TODO FIX NOW: EventSize does not include the size of the int that indicates the number of stack frames. 
            Debug.Assert(eventData->PayloadSize + EventPipeEventHeader.HeaderSize + EventPipeEventHeader.StackBytesSize(eventData) == eventData->EventSize);

            TraceEventNativeMethods.EVENT_RECORD* ret = null;
            EventPipeEventMetaData metaData;
            if (eventData->MetaDataId == 0)     // Is this a Meta-data event?  
            {               
                int eventSize = eventData->EventSize;
                int payloadSize = eventData->PayloadSize;
                StreamLabel metaDataStreamOffset = reader.Current;  // Used as the 'id' for the meta-data
                // Note that this skip invalidates the eventData pointer, so it is important to pull any fields out we need first.  
                reader.Skip(EventPipeEventHeader.HeaderSize);
                metaData = new EventPipeEventMetaData(reader, payloadSize, _fileFormatVersionNumber, PointerSize, _processId);
                _eventMetadataDictionary.Add(metaDataStreamOffset, metaData);
                _eventParser.AddTemplate(metaData);
                int stackBytes = reader.ReadInt32();        // Meta-data events should always have a empty stack.  
                Debug.Assert(stackBytes == 0);

                // We have read all the bytes in the event as given by the EventSize  
                // FIX NOW the sizeof Int is because eventSize does not include the int that indicate no stack.  
                Debug.Assert(reader.Current == metaDataStreamOffset.Add(eventSize + sizeof(int)));
            }
            else
            {

                if (_eventMetadataDictionary.TryGetValue(eventData->MetaDataId, out metaData))
                    ret = metaData.GetEventRecordForEventData(eventData, PointerSize);
                else
                    Debug.Assert(false, "Warning can't find metaData for ID " + eventData->MetaDataId.ToString("x"));

                // TODO FIX NOW EventSize should include the sizeof(int) so we don't need to put it here. 
                Debug.Assert(eventData->PayloadSize + EventPipeEventHeader.HeaderSize + sizeof(int) + EventPipeEventHeader.StackBytesSize(eventData) == eventData->EventSize + sizeof(int));
                reader.Skip(eventData->EventSize + sizeof(int));
            }

            return ret;
        }

        internal override unsafe Guid GetRelatedActivityID(TraceEventNativeMethods.EVENT_RECORD* eventRecord)
        {
            // Recover the EventPipeEventHeader from the payload pointer and then fetch from the header.  
            EventPipeEventHeader* event_ = EventPipeEventHeader.HeaderFromPayloadPointer((byte*)eventRecord->UserData);
            return event_->RelatedActivityID;
        }

        // We dont ever serialize one of these in managed code so we don't need to implement ToSTream
        public void ToStream(Serializer serializer)
        {
            throw new NotImplementedException();
        }

        public void FromStream(Deserializer deserializer)
        {
            _fileFormatVersionNumber = deserializer.VersionBeingRead;

            ForwardReference reference = deserializer.ReadForwardReference();
            _endOfEventStream = deserializer.ResolveForwardReference(reference, preserveCurrent: true);

            // The start time is stored as a SystemTime which is a bunch of shorts, convert to DateTime.  
            short year = deserializer.ReadInt16();
            short month = deserializer.ReadInt16();
            short dayOfWeek = deserializer.ReadInt16();
            short day = deserializer.ReadInt16();
            short hour = deserializer.ReadInt16();
            short minute = deserializer.ReadInt16();
            short second = deserializer.ReadInt16();
            short milliseconds = deserializer.ReadInt16();
            _syncTimeUTC = new DateTime(year, month, day, hour, minute, second, milliseconds, DateTimeKind.Utc);
            deserializer.Read(out _syncTimeQPC);
            deserializer.Read(out _QPCFreq);

            sessionStartTimeQPC = _syncTimeQPC;
            _startEventOfStream = deserializer.Current;      // Events immediately after the header.  
        }

        int _fileFormatVersionNumber;
        StreamLabel _startEventOfStream;
        StreamLabel _endOfEventStream;

        Dictionary<StreamLabel, EventPipeEventMetaData> _eventMetadataDictionary = new Dictionary<StreamLabel, EventPipeEventMetaData>();
        Deserializer _deserializer;
        EventPipeTraceEventParser _eventParser; // TODO does this belong here?
        string _processName;
        int _processId;

        #endregion
    }

    unsafe class EventPipeEventMetaData
    {
        /// <summary>
        /// Creates a new MetaData instance from the serialized data at the current position of 'reader'
        /// of length 'length'.   This typically point at the PAYLOAD AREA of a meta-data events)
        /// 'fileFormatVersionNumber' is the version number of the file as a whole
        /// (since that affects the parsing of this data).   When this constructor returns the reader
        /// has read all data given to it (thus it has move the read pointer by 'length'.  
        /// </summary>
        public EventPipeEventMetaData(PinnedStreamReader reader, int length, int fileFormatVersionNumber, int pointerSize, int processId)
        {
            StreamLabel eventDataEnd = reader.Current.Add(length);

            _eventRecord = (TraceEventNativeMethods.EVENT_RECORD*)Marshal.AllocHGlobal(sizeof(TraceEventNativeMethods.EVENT_RECORD));
            ClearMemory(_eventRecord, sizeof(TraceEventNativeMethods.EVENT_RECORD));

            if (pointerSize == 4)
                _eventRecord->EventHeader.Flags = TraceEventNativeMethods.EVENT_HEADER_FLAG_32_BIT_HEADER;
            else
                _eventRecord->EventHeader.Flags = TraceEventNativeMethods.EVENT_HEADER_FLAG_64_BIT_HEADER;

            _eventRecord->EventHeader.ProcessId = processId;

            StreamLabel metaDataStart = reader.Current;
            if (fileFormatVersionNumber == 1)
                _eventRecord->EventHeader.ProviderId = reader.ReadGuid();
            else
            {
                ProviderName = reader.ReadNullTerminatedUnicodeString();
                _eventRecord->EventHeader.ProviderId = GetProviderGuidFromProviderName(ProviderName);
            }

            var eventId = (ushort)reader.ReadInt32();
            _eventRecord->EventHeader.Id = eventId;
            Debug.Assert(_eventRecord->EventHeader.Id == eventId);  // No trucation

            var version = reader.ReadInt32();
            _eventRecord->EventHeader.Version = (byte)version;
            Debug.Assert(_eventRecord->EventHeader.Version == version);  // No trucation

            int metadataLength = reader.ReadInt32();
            Debug.Assert(0 <= metadataLength && metadataLength < length);
            if (0 < metadataLength)
            {
                // TODO why do we repeat the event number it is redundant.  
                eventId = (ushort)reader.ReadInt32();
                Debug.Assert(_eventRecord->EventHeader.Id == eventId);  // No trucation
                EventName = reader.ReadNullTerminatedUnicodeString();
                Debug.Assert(EventName.Length < length / 2);

                // Deduce the opcode from the name.   
                if (EventName.EndsWith("Start", StringComparison.OrdinalIgnoreCase))
                    _eventRecord->EventHeader.Opcode = (byte)TraceEventOpcode.Start;
                else if (EventName.EndsWith("Stop", StringComparison.OrdinalIgnoreCase))
                    _eventRecord->EventHeader.Opcode = (byte)TraceEventOpcode.Stop;

                _eventRecord->EventHeader.Keyword = (ulong)reader.ReadInt64();

                // TODO why do we repeat the event number it is redundant.  
                version = reader.ReadInt32();
                Debug.Assert(_eventRecord->EventHeader.Version == version);     // No trucation

                _eventRecord->EventHeader.Level = (byte)reader.ReadInt32();
                Debug.Assert(_eventRecord->EventHeader.Level <= 5);

                // Fetch the parameter information
                int parameterCount = reader.ReadInt32();
                Debug.Assert(0 <= parameterCount && parameterCount < length / 8); // Each parameter takes at least 8 bytes.  
                if (parameterCount > 0)
                {
                    ParameterDefinitions = new Tuple<TypeCode, string>[parameterCount];
                    for (int i = 0; i < parameterCount; i++)
                    {
                        var type = (TypeCode)reader.ReadInt32();
                        Debug.Assert((uint)type < 24);      // There only a handful of type codes. 
                        var name = reader.ReadNullTerminatedUnicodeString();
                        ParameterDefinitions[i] = new Tuple<TypeCode, string>(type, name);
                        Debug.Assert(reader.Current <= eventDataEnd);
                    }
                }
            }
            Debug.Assert(reader.Current == eventDataEnd);
        }

        internal TraceEventNativeMethods.EVENT_RECORD* GetEventRecordForEventData(EventPipeEventHeader* eventData, int pointerSize)
        {

            // We have already initialize all the fields of _eventRecord that do no vary from event to event. 
            // Now we only have to copy over the fields that are specific to particular event.  
            _eventRecord->EventHeader.ThreadId = eventData->ThreadId;
            _eventRecord->EventHeader.TimeStamp = eventData->TimeStamp;
            _eventRecord->EventHeader.ActivityId = eventData->ActivityID;
            // EVENT_RECORD does not field for ReleatedActivityID (because it is rarely used).  See GetRelatedActivityID;
            _eventRecord->UserDataLength = (ushort)eventData->PayloadSize;

            // TODO the extra || operator is a hack becase the runtime actually tries to emit events that
            // exceed this for the GC/BulkSurvivingObjectRanges (event id == 21).  We supress that assert 
            // for now but this is a real bug in the runtime's event logging.  ETW can't handle payloads > 64K.  
            Debug.Assert(_eventRecord->UserDataLength == eventData->PayloadSize ||
                _eventRecord->EventHeader.ProviderId == ClrTraceEventParser.ProviderGuid && _eventRecord->EventHeader.Id == 21);
            _eventRecord->UserData = (IntPtr)eventData->Payload;

            int stackBytesSize = EventPipeEventHeader.StackBytesSize(eventData);

            // TODO remove once .NET Core has been fixed to not emit stacks on CLR method events which are just for bookeeping.  
            if (ProviderId == ClrRundownTraceEventParser.ProviderGuid ||
               (ProviderId == ClrTraceEventParser.ProviderGuid && (140 <= EventId && EventId <= 144 || EventId == 190)))     // These are various CLR method Events.  
                stackBytesSize = 0;

            if (0 < stackBytesSize)
            {
                // Lazy allocation (destructor frees it). 
                if (_eventRecord->ExtendedData == null)
                    _eventRecord->ExtendedData = (TraceEventNativeMethods.EVENT_HEADER_EXTENDED_DATA_ITEM*)Marshal.AllocHGlobal(sizeof(TraceEventNativeMethods.EVENT_HEADER_EXTENDED_DATA_ITEM));

                // Hook in the stack data.  
                _eventRecord->ExtendedData->ExtType = pointerSize == 4 ? TraceEventNativeMethods.EVENT_HEADER_EXT_TYPE_STACK_TRACE32 : TraceEventNativeMethods.EVENT_HEADER_EXT_TYPE_STACK_TRACE64;

                // DataPtr should point at a EVENT_EXTENDED_ITEM_STACK_TRACE*.  These have a ulong MatchID field which is NOT USED before the stack data.
                // Since that field is not used, I can backup the pointer by 8 bytes and synthesize a EVENT_EXTENDED_ITEM_STACK_TRACE from the raw buffer 
                // of stack data without having to copy.  
                _eventRecord->ExtendedData->DataSize = (ushort)(stackBytesSize + 8);
                _eventRecord->ExtendedData->DataPtr = (ulong)(EventPipeEventHeader.StackBytes(eventData) - 8);

                _eventRecord->ExtendedDataCount = 1;        // Mark that we have the stack data.  
            }
            else
                _eventRecord->ExtendedDataCount = 0;

            return _eventRecord;
        }

        public string ProviderName { get; private set; }
        public string EventName { get; private set; }
        public Tuple<TypeCode, string>[] ParameterDefinitions { get; private set; }
        public Guid ProviderId { get { return _eventRecord->EventHeader.ProviderId; } }
        public int EventId { get { return _eventRecord->EventHeader.Id; } }
        public int Version { get { return _eventRecord->EventHeader.Version; } }
        public ulong Keywords { get { return _eventRecord->EventHeader.Keyword; } }
        public int Level { get { return _eventRecord->EventHeader.Level; } }

        #region private 
        ~EventPipeEventMetaData()
        {
            if (_eventRecord != null)
            {
                if (_eventRecord->ExtendedData != null)
                    Marshal.FreeHGlobal((IntPtr)_eventRecord->ExtendedData);
                Marshal.FreeHGlobal((IntPtr)_eventRecord);
                _eventRecord = null;
            }

        }

        private void ClearMemory(void* buffer, int length)
        {
            byte* ptr = (byte*)buffer;
            while (length > 0)
            {
                *ptr++ = 0;
                --length;
            }

        }
        public static Guid GetProviderGuidFromProviderName(string name)
        {
            if (String.IsNullOrEmpty(name))
            {
                return Guid.Empty;
            }

            // Legacy GUID lookups (events which existed before the current Guid generation conventions)
            if (name == TplEtwProviderTraceEventParser.ProviderName)
            {
                return TplEtwProviderTraceEventParser.ProviderGuid;
            }
            else if (name == ClrTraceEventParser.ProviderName)
            {
                return ClrTraceEventParser.ProviderGuid;
            }
            else if (name == ClrPrivateTraceEventParser.ProviderName)
            {
                return ClrPrivateTraceEventParser.ProviderGuid;
            }
            else if (name == ClrRundownTraceEventParser.ProviderName)
            {
                return ClrRundownTraceEventParser.ProviderGuid;
            }
            else if (name == ClrStressTraceEventParser.ProviderName)
            {
                return ClrStressTraceEventParser.ProviderGuid;
            }
            else if (name == FrameworkEventSourceTraceEventParser.ProviderName)
            {
                return FrameworkEventSourceTraceEventParser.ProviderGuid;
            }
            // Needed as long as eventpipeinstance v1 objects are supported
            else if (name == SampleProfilerTraceEventParser.ProviderName)
            {
                return SampleProfilerTraceEventParser.ProviderGuid;
            }

            // Hash the name according to current event source naming conventions
            else
            {
                return TraceEventProviders.GetEventSourceGuidFromName(name);
            }
        }


        TraceEventNativeMethods.EVENT_RECORD* _eventRecord;
        #endregion
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    unsafe struct EventPipeEventHeader
    {
        public int EventSize;           // Size bytes of this header and the payload and stacks if any 
        public StreamLabel MetaDataId;  // a number identifying the description of this event.  It is a stream location. 
        public int ThreadId;
        public long TimeStamp;
        public Guid ActivityID;
        public Guid RelatedActivityID;
        public int PayloadSize;         // size in bytes of the user defined payload data. 
        public fixed byte Payload[4];     // Actually of variable size.  4 is used to avoid potential alignment issues.   This 4 also appears in HeaderSize below. 

        /// <summary>
        /// Header Size is defined to be the number of bytes before the Payload bytes.  
        /// </summary>
        static public int HeaderSize { get { return sizeof(EventPipeEventHeader) - 4; } }
        static public EventPipeEventHeader* HeaderFromPayloadPointer(byte* payloadPtr) { return (EventPipeEventHeader*)(payloadPtr - HeaderSize); }

        static public int StackBytesSize(EventPipeEventHeader* header)
        {
            return *((int*)(&header->Payload[header->PayloadSize]));
        }
        static public byte* StackBytes(EventPipeEventHeader* header)
        {
            return (byte*)(&header->Payload[header->PayloadSize + 4]);
        }
    }
}

#if !OLD
namespace Microsoft.Diagnostics.Tracing.EventPipe
{
    public abstract class EventPipeEventSource : TraceEventDispatcher
    {
        public EventPipeEventSource(Deserializer deserializer)
        {
            if (deserializer == null)
            {
                throw new ArgumentNullException(nameof(deserializer));
            }

            _deserializer = deserializer;
        }

        ~EventPipeEventSource()
        {
            Dispose(false);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _deserializer?.Dispose();
            }

            base.Dispose(disposing);
            GC.SuppressFinalize(this);
        }

        #region Private
        protected Deserializer _deserializer;
        #endregion

    }
}
#endif