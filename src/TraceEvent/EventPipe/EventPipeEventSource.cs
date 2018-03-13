using FastSerialization;
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
    /// <summary>
    /// EventPipeEventSource knows how to decode EventPipe (generated by the .NET core runtime).
    /// Please see <see href="https://github.com/Microsoft/perfview/blob/master/src/TraceEvent/EventPipe/EventPipeFormat.md" />for details on the file format.
    /// 
    /// By conventions files of such a format are given the .netperf suffix and are logically
    /// very much like a ETL file in that they have a header that indicete things about
    /// the trace as a whole, and a list of events.    Like more modern ETL files the
    /// file as a whole is self-describing.    Some of the events are 'MetaData' events
    /// that indicate the provider name, event name, and payload field names and types.   
    /// Ordinary events then point at these meta-data event so that logically all 
    /// events have a name some basic information (process, thread, timestamp, activity
    /// ID) and user defined field names and values of various types.  
    /// </summary>
    unsafe public class EventPipeEventSource : TraceEventDispatcher, IFastSerializable, IFastSerializableVersion
    {
        public EventPipeEventSource(string fileName)
        {
            _processName = "ProcessBeingTraced";
            osVersion = new Version("0.0.0.0");
            cpuSpeedMHz = 10;

            _deserializer = new Deserializer(new PinnedStreamReader(fileName, 0x20000), fileName);

#if SUPPORT_V1_V2
            // This is only here for V2 and V1.  V3+ should use the name EventTrace, it can be removed when we drop support.
            _deserializer.RegisterFactory("Microsoft.DotNet.Runtime.EventPipeFile", delegate { return this; });
#endif
            _deserializer.RegisterFactory("Trace", delegate { return this; });
            _deserializer.RegisterFactory("EventBlock", delegate { return new EventPipeEventBlock(this); });

            var entryObj = _deserializer.GetEntryObject(); // this call invokes FromStream and reads header data

            // Because we told the deserialize to use 'this' when creating a EventPipeFile, we 
            // expect the entry object to be 'this'.
            Debug.Assert(entryObj == this);

            _eventParser = new EventPipeTraceEventParser(this);
        }

        #region private
        // I put these in the private section because they are overrides, and thus don't ADD to the API.  
        public override int EventsLost => 0;

        /// <summary>
        /// This is the version number reader and writer (although we don't don't have a writer at the moment)
        /// It MUST be updated (as well as MinimumReaderVersion), if breaking changes have been made.
        /// If your changes are forward compatible (old readers can still read the new format) you 
        /// don't have to update the version number but it is useful to do so (while keeping MinimumReaderVersion unchanged)
        /// so that readers can quickly determine what new content is available.  
        /// </summary>
        public int Version => 3;

        /// <summary>
        /// This field is only used for writers, and this code does not have writers so it is not used.
        /// It should be set to Version unless changes since the last version are forward compatible
        /// (old readers can still read this format), in which case this shoudl be unchanged.  
        /// </summary>
        public int MinimumReaderVersion => Version;

        /// <summary>
        /// This is the smallest version that the deserializer here can read.   Currently 
        /// we are careful about backward compat so our deserializer can read anything that
        /// has ever been produced.   We may change this when we believe old writers basically
        /// no longer exist (and we can remove that support code). 
        /// </summary>
        public int MinimumVersionCanRead => 0;

        protected override void Dispose(bool disposing)
        {
            _deserializer.Dispose();

            base.Dispose(disposing);
        }

        public override bool Process()
        {
            if (_fileFormatVersionNumber >= 3)
            {
                // loop through the stream until we hit a null object.  Deserialization of 
                // EventPipeEventBlocks will cause dispatch to happen.  
                // ReadObject uses registered factories and recognizes types by names, then derserializes them with FromStream
                while (_deserializer.ReadObject() != null)
                { }
            }
#if SUPPORT_V1_V2
            else
            {
                PinnedStreamReader deserializerReader = (PinnedStreamReader)_deserializer.Reader;
                while (deserializerReader.Current < _endOfEventStream)
                {
                    TraceEventNativeMethods.EVENT_RECORD* eventRecord = ReadEvent(deserializerReader);
                    if (eventRecord != null)
                    {
                        // in the code below we set sessionEndTimeQPC to be the timestamp of the last event.  
                        // Thus the new timestamp should be later, and not more than 1 day later.  
                        Debug.Assert(sessionEndTimeQPC <= eventRecord->EventHeader.TimeStamp);
                        Debug.Assert(sessionEndTimeQPC == 0 || eventRecord->EventHeader.TimeStamp - sessionEndTimeQPC < _QPCFreq * 24 * 3600);

                        var traceEvent = Lookup(eventRecord);
                        Dispatch(traceEvent);
                        sessionEndTimeQPC = eventRecord->EventHeader.TimeStamp;
                    }
                }
            }
#endif
            return true;
        }

        internal override string ProcessName(int processID, long timeQPC) => _processName;

        internal TraceEventNativeMethods.EVENT_RECORD* ReadEvent(PinnedStreamReader reader)
        {
            EventPipeEventHeader* eventData = (EventPipeEventHeader*)reader.GetPointer(EventPipeEventHeader.HeaderSize);
            eventData = (EventPipeEventHeader*)reader.GetPointer(eventData->TotalEventSize); // now we now the real size and get read entire event

            // Basic sanity checks.  Are the timestamps and sizes sane.  
            Debug.Assert(sessionEndTimeQPC <= eventData->TimeStamp);
            Debug.Assert(sessionEndTimeQPC == 0 || eventData->TimeStamp - sessionEndTimeQPC < _QPCFreq * 24 * 3600);
            Debug.Assert(0 <= eventData->PayloadSize && eventData->PayloadSize <= eventData->TotalEventSize);
            Debug.Assert(0 < eventData->TotalEventSize && eventData->TotalEventSize < 0x20000);  // TODO really should be 64K but BulkSurvivingObjectRanges needs fixing.
            Debug.Assert(_fileFormatVersionNumber < 3 ||
                ((int)EventPipeEventHeader.PayloadBytes(eventData) % 4 == 0 && eventData->TotalEventSize % 4 == 0)); // ensure 4 byte alignment

            StreamLabel eventDataEnd = reader.Current.Add(eventData->TotalEventSize);

            Debug.Assert(0 <= EventPipeEventHeader.StackBytesSize(eventData) && EventPipeEventHeader.StackBytesSize(eventData) <= eventData->TotalEventSize);

            TraceEventNativeMethods.EVENT_RECORD* ret = null;
            if (eventData->IsMetadata())
            {
                int totalEventSize = eventData->TotalEventSize;
                int payloadSize = eventData->PayloadSize;

                // Note that this skip invalidates the eventData pointer, so it is important to pull any fields out we need first.  
                reader.Skip(EventPipeEventHeader.HeaderSize);

                var metaData = new EventPipeEventMetaData(reader, payloadSize, _fileFormatVersionNumber, PointerSize, _processId);
                _eventMetadataDictionary.Add(metaData.MetaDataId, metaData);

                _eventParser.AddTemplate(metaData); // if we don't add the templates to this parse, we are going to have unhadled events (see https://github.com/Microsoft/perfview/issues/461)

                int stackBytes = reader.ReadInt32();
                Debug.Assert(stackBytes == 0, "Meta-data events should always have a empty stack");
            }
            else
            {
                if (_eventMetadataDictionary.TryGetValue(eventData->MetaDataId, out var metaData))
                    ret = metaData.GetEventRecordForEventData(eventData);
                else
                    Debug.Assert(false, "Warning can't find metaData for ID " + eventData->MetaDataId.ToString("x"));
            }

            reader.Goto(eventDataEnd);

            return ret;
        }

        internal override unsafe Guid GetRelatedActivityID(TraceEventNativeMethods.EVENT_RECORD* eventRecord)
        {
            // Recover the EventPipeEventHeader from the payload pointer and then fetch from the header.  
            EventPipeEventHeader* event_ = EventPipeEventHeader.HeaderFromPayloadPointer((byte*)eventRecord->UserData);
            return event_->RelatedActivityID;
        }

        public void ToStream(Serializer serializer) => throw new InvalidOperationException("We dont ever serialize one of these in managed code so we don't need to implement ToSTream");

        public void FromStream(Deserializer deserializer)
        {
            _fileFormatVersionNumber = deserializer.VersionBeingRead;

#if SUPPORT_V1_V2
            if (deserializer.VersionBeingRead < 3)
            {
                ForwardReference reference = deserializer.ReadForwardReference();
                _endOfEventStream = deserializer.ResolveForwardReference(reference, preserveCurrent: true);
            }
#endif
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

            if (3 <= deserializer.VersionBeingRead)
            {
                deserializer.Read(out pointerSize);
                deserializer.Read(out _processId);
                deserializer.Read(out numberOfProcessors);
                deserializer.Read(out _expectedCPUSamplingRate);
            }
#if SUPPORT_V1_V2
            else
            {
                _processId = 0; // V1 && V2 tests expect 0 for process Id
                pointerSize = 8; // V1 EventPipe only supports Linux which is x64 only.
                numberOfProcessors = 1;
            }
#endif
        }

#if SUPPORT_V1_V2
        StreamLabel _endOfEventStream;
#endif
        int _fileFormatVersionNumber;
        Dictionary<int, EventPipeEventMetaData> _eventMetadataDictionary = new Dictionary<int, EventPipeEventMetaData>();
        Deserializer _deserializer;
        EventPipeTraceEventParser _eventParser; // TODO does this belong here?
        string _processName;
        internal int _processId;
        internal int _expectedCPUSamplingRate;
        #endregion
    }

    #region private classes

    /// <summary>
    /// An EVentPipeEventBlock represents a block of events.   It basicaly only has
    /// one field, which is the size in bytes of the block.  But when its FromStream
    /// is called, it will perform the callbacks for the events (thus deserializing
    /// it performs dispatch).  
    /// </summary>
    internal class EventPipeEventBlock : IFastSerializable
    {
        public EventPipeEventBlock(EventPipeEventSource source) => _source = source;

        unsafe public void FromStream(Deserializer deserializer)
        {
            // blockSizeInBytes INCLUDES any padding bytes to ensure alignment.  
            var blockSizeInBytes = deserializer.ReadInt();

            // after the block size comes eventual padding, we just need to skip it by jumping to the nearest aligned address
            if ((int)deserializer.Current % 4 != 0)
            {
                var nearestAlignedAddress = deserializer.Current.Add(4 - ((int)deserializer.Current % 4));
                deserializer.Goto(nearestAlignedAddress);
            }

            _startEventData = deserializer.Current;
            _endEventData = _startEventData.Add(blockSizeInBytes);
            Debug.Assert((int)_startEventData % 4 == 0 && (int)_endEventData % 4 == 0); // make sure that the data is aligned

            // Dispatch through all the events.  
            PinnedStreamReader deserializerReader = (PinnedStreamReader)deserializer.Reader;

            while (deserializerReader.Current < _endEventData)
            {
                TraceEventNativeMethods.EVENT_RECORD* eventRecord = _source.ReadEvent(deserializerReader);
                if (eventRecord != null)
                {
                    // in the code below we set sessionEndTimeQPC to be the timestamp of the last event.  
                    // Thus the new timestamp should be later, and not more than 1 day later.  
                    Debug.Assert(_source.sessionEndTimeQPC <= eventRecord->EventHeader.TimeStamp);
                    Debug.Assert(_source.sessionEndTimeQPC == 0 || eventRecord->EventHeader.TimeStamp - _source.sessionEndTimeQPC < _source._QPCFreq * 24 * 3600);

                    var traceEvent = _source.Lookup(eventRecord);
                    _source.Dispatch(traceEvent);
                    _source.sessionEndTimeQPC = eventRecord->EventHeader.TimeStamp;
                }
            }

            deserializerReader.Goto(_endEventData); // go to the end of block, in case some padding was not skipped yet
        }

        public void ToStream(Serializer serializer) => throw new InvalidOperationException();

        StreamLabel _startEventData;
        StreamLabel _endEventData;
        EventPipeEventSource _source;
    }

    /// <summary>
    /// Private utility class.
    /// 
    /// An EventPipeEventMetaData holds the information that can be shared among all
    /// instances of an EventPIpe event from a particular provider.   Thus it contains
    /// things like the event name, provider, as well as well as data on how many
    /// user defined fields and their names and types.   
    /// 
    /// This class has two main functions
    ///    1. The constructor takes a PinnedStreamReader and decodes the serialized metadata
    ///       so you can access the data conviniently.
    ///    2. It remembers a EVENT_RECORD structure (from ETW) that contains this data)
    ///       and has a function GetEventRecordForEventData which converts from a 
    ///       EventPipeEventHeader (the raw serialized data) to a EVENT_RECORD (which
    ///       is what TraceEvent needs to look up the event an pass it up the stack.  
    /// </summary>
    unsafe class EventPipeEventMetaData
    {
        /// <summary>
        /// Creates a new MetaData instance from the serialized data at the current position of 'reader'
        /// of length 'length'.   This typically points at the PAYLOAD AREA of a meta-data events)
        /// 'fileFormatVersionNumber' is the version number of the file as a whole
        /// (since that affects the parsing of this data) and 'processID' is the process ID for the 
        /// whole stream (since it needs to be put into the EVENT_RECORD.
        /// 
        /// When this constructor returns the reader has read all data given to it (thus it has
        /// move the read pointer by 'length')
        /// </summary>
        public EventPipeEventMetaData(PinnedStreamReader reader, int length, int fileFormatVersionNumber, int pointerSize, int processId)
        {
            // Get the event record and fill in fields that we can without deserializing anything.  
            _eventRecord = (TraceEventNativeMethods.EVENT_RECORD*)Marshal.AllocHGlobal(sizeof(TraceEventNativeMethods.EVENT_RECORD));
            ClearMemory(_eventRecord, sizeof(TraceEventNativeMethods.EVENT_RECORD));

            if (pointerSize == 4)
                _eventRecord->EventHeader.Flags = TraceEventNativeMethods.EVENT_HEADER_FLAG_32_BIT_HEADER;
            else
                _eventRecord->EventHeader.Flags = TraceEventNativeMethods.EVENT_HEADER_FLAG_64_BIT_HEADER;

            _eventRecord->EventHeader.ProcessId = processId;

            // Read the metaData
            StreamLabel eventDataEnd = reader.Current.Add(length);
            if (3 <= fileFormatVersionNumber)
            {
                MetaDataId = reader.ReadInt32();
                ProviderName = reader.ReadNullTerminatedUnicodeString();
                _eventRecord->EventHeader.ProviderId = GetProviderGuidFromProviderName(ProviderName);

                ReadEventMetaData(reader, fileFormatVersionNumber);
            }
#if SUPPORT_V1_V2
            else
                ReadObsoleteEventMetaData(reader, fileFormatVersionNumber);
#endif

            Debug.Assert(reader.Current == eventDataEnd);
        }

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

        /// <summary>
        /// Given a EventPipeEventHeader takes a EventPipeEventHeader that is specific to an event, copies it
        /// on top of the static information in its EVENT_RECORD which is specialized this this meta-data 
        /// and returns a pinter to it.  Thus this makes the EventPipe look like an ETW provider from
        /// the point of view of the upper level TraceEvent logic.  
        /// </summary>
        internal TraceEventNativeMethods.EVENT_RECORD* GetEventRecordForEventData(EventPipeEventHeader* eventData)
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

                if ((_eventRecord->EventHeader.Flags & TraceEventNativeMethods.EVENT_HEADER_FLAG_32_BIT_HEADER) != 0)
                    _eventRecord->ExtendedData->ExtType = TraceEventNativeMethods.EVENT_HEADER_EXT_TYPE_STACK_TRACE32;
                else
                    _eventRecord->ExtendedData->ExtType = TraceEventNativeMethods.EVENT_HEADER_EXT_TYPE_STACK_TRACE64;

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

        /// <summary>
        /// This is a number that is unique to this meta-data blob.  It is expected to be a small integer
        /// that starts at 1 (since 0 is reserved) and increases from there (thus an array can be used).  
        /// It is what is matched up with EventPipeEventHeader.MetaDataId
        /// </summary>
        public int MetaDataId { get; private set; }
        public string ProviderName { get; private set; }
        public string EventName { get; private set; }
#if SUPPORT_V1_V2
        public Tuple<TypeCode, string>[] ParameterDefinitions { get; private set; }
#endif
        public DynamicTraceEventData Template { get; private set; }
        public Guid ProviderId { get { return _eventRecord->EventHeader.ProviderId; } }
        public int EventId { get { return _eventRecord->EventHeader.Id; } }
        public int Version { get { return _eventRecord->EventHeader.Version; } }
        public ulong Keywords { get { return _eventRecord->EventHeader.Keyword; } }
        public int Level { get { return _eventRecord->EventHeader.Level; } }

        /// <summary>
        /// Reads the meta data for information specific to one event.  
        /// </summary>
        private void ReadEventMetaData(PinnedStreamReader reader, int fileFormatVersionNumber)
        {
            int eventId = (ushort)reader.ReadInt32();
            _eventRecord->EventHeader.Id = (ushort)eventId;
            Debug.Assert(_eventRecord->EventHeader.Id == eventId);  // No truncation

            EventName = reader.ReadNullTerminatedUnicodeString();

            // Deduce the opcode from the name.   
            if (EventName.EndsWith("Start", StringComparison.OrdinalIgnoreCase))
                _eventRecord->EventHeader.Opcode = (byte)TraceEventOpcode.Start;
            else if (EventName.EndsWith("Stop", StringComparison.OrdinalIgnoreCase))
                _eventRecord->EventHeader.Opcode = (byte)TraceEventOpcode.Stop;

            _eventRecord->EventHeader.Keyword = (ulong)reader.ReadInt64();

            int version = reader.ReadInt32();
            _eventRecord->EventHeader.Version = (byte)version;
            Debug.Assert(_eventRecord->EventHeader.Version == version);  // No truncation

            _eventRecord->EventHeader.Level = (byte)reader.ReadInt32();
            Debug.Assert(_eventRecord->EventHeader.Level <= 5);

            // Fetch the parameter information
            Template = ReadMetadataAndBuildTemplate(reader);
        }

        private DynamicTraceEventData ReadMetadataAndBuildTemplate(PinnedStreamReader reader)
        {
            int opcode;
            string opcodeName;

            EventPipeTraceEventParser.GetOpcodeFromEventName(EventName, out opcode, out opcodeName);

            DynamicTraceEventData.PayloadFetchClassInfo classInfo = null;
            DynamicTraceEventData template = new DynamicTraceEventData(null, EventId, 0, EventName, Guid.Empty, opcode, opcodeName, ProviderId, ProviderName);

            // Read the count of event payload fields.
            int fieldCount = reader.ReadInt32();
            Debug.Assert(0 <= fieldCount && fieldCount < 0x4000);

            if (fieldCount > 0)
            {
                // Recursively parse the metadata, building up a list of payload names and payload field fetch objects.
                classInfo = ParseFields(reader, fieldCount);
            }
            else
            {
                classInfo = new DynamicTraceEventData.PayloadFetchClassInfo()
                {
                    FieldNames = new string[0],
                    FieldFetches = new DynamicTraceEventData.PayloadFetch[0]
                };
            }

            template.payloadNames = classInfo.FieldNames;
            template.payloadFetches = classInfo.FieldFetches;

            return template;
        }

        private DynamicTraceEventData.PayloadFetchClassInfo ParseFields(PinnedStreamReader reader, int numFields)
        {
            string[] fieldNames = new string[numFields];
            DynamicTraceEventData.PayloadFetch[] fieldFetches = new DynamicTraceEventData.PayloadFetch[numFields];

            ushort offset = 0;
            for (int fieldIndex = 0; fieldIndex < numFields; fieldIndex++)
            {
                DynamicTraceEventData.PayloadFetch payloadFetch = new DynamicTraceEventData.PayloadFetch();

                // Read the TypeCode for the current field.
                TypeCode typeCode = (TypeCode)reader.ReadInt32();

                // Fill out the payload fetch object based on the TypeCode.
                switch (typeCode)
                {
                    case TypeCode.Boolean:
                        {
                            payloadFetch.Type = typeof(bool);
                            payloadFetch.Size = 4; // We follow windows conventions and use 4 bytes for bool.
                            payloadFetch.Offset = offset;
                            break;
                        }
                    case TypeCode.Char:
                        {
                            payloadFetch.Type = typeof(char);
                            payloadFetch.Size = sizeof(char);
                            payloadFetch.Offset = offset;
                            break;
                        }
                    case TypeCode.SByte:
                        {
                            payloadFetch.Type = typeof(SByte);
                            payloadFetch.Size = sizeof(SByte);
                            payloadFetch.Offset = offset;
                            break;
                        }
                    case TypeCode.Byte:
                        {
                            payloadFetch.Type = typeof(byte);
                            payloadFetch.Size = sizeof(byte);
                            payloadFetch.Offset = offset;
                            break;
                        }
                    case TypeCode.Int16:
                        {
                            payloadFetch.Type = typeof(Int16);
                            payloadFetch.Size = sizeof(Int16);
                            payloadFetch.Offset = offset;
                            break;
                        }
                    case TypeCode.UInt16:
                        {
                            payloadFetch.Type = typeof(UInt16);
                            payloadFetch.Size = sizeof(UInt16);
                            payloadFetch.Offset = offset;
                            break;
                        }
                    case TypeCode.Int32:
                        {
                            payloadFetch.Type = typeof(Int32);
                            payloadFetch.Size = sizeof(Int32);
                            payloadFetch.Offset = offset;
                            break;
                        }
                    case TypeCode.UInt32:
                        {
                            payloadFetch.Type = typeof(UInt32);
                            payloadFetch.Size = sizeof(UInt32);
                            payloadFetch.Offset = offset;
                            break;
                        }
                    case TypeCode.Int64:
                        {
                            payloadFetch.Type = typeof(Int64);
                            payloadFetch.Size = sizeof(Int64);
                            payloadFetch.Offset = offset;
                            break;
                        }
                    case TypeCode.UInt64:
                        {
                            payloadFetch.Type = typeof(UInt64);
                            payloadFetch.Size = sizeof(UInt64);
                            payloadFetch.Offset = offset;
                            break;
                        }
                    case TypeCode.Single:
                        {
                            payloadFetch.Type = typeof(Single);
                            payloadFetch.Size = sizeof(Single);
                            payloadFetch.Offset = offset;
                            break;
                        }
                    case TypeCode.Double:
                        {
                            payloadFetch.Type = typeof(Double);
                            payloadFetch.Size = sizeof(Double);
                            payloadFetch.Offset = offset;
                            break;
                        }
                    case TypeCode.Decimal:
                        {
                            payloadFetch.Type = typeof(Decimal);
                            payloadFetch.Size = sizeof(Decimal);
                            payloadFetch.Offset = offset;
                            break;
                        }
                    case TypeCode.DateTime:
                        {
                            payloadFetch.Type = typeof(DateTime);
                            payloadFetch.Size = 8;
                            payloadFetch.Offset = offset;
                            break;
                        }
                    case EventPipeTraceEventParser.GuidTypeCode:
                        {
                            payloadFetch.Type = typeof(Guid);
                            payloadFetch.Size = 16;
                            payloadFetch.Offset = offset;
                            break;
                        }
                    case TypeCode.String:
                        {
                            payloadFetch.Type = typeof(String);
                            payloadFetch.Size = DynamicTraceEventData.NULL_TERMINATED;
                            payloadFetch.Offset = offset;
                            break;
                        }
                    case TypeCode.Object:
                        {
                            // TypeCode.Object represents an embedded struct.

                            // Read the number of fields in the struct.  Each of these fields could be an embedded struct,
                            // but these embedded structs are still counted as single fields.  They will be expanded when they are handled.
                            int structFieldCount = reader.ReadInt32();
                            DynamicTraceEventData.PayloadFetchClassInfo embeddedStructClassInfo = ParseFields(reader, structFieldCount);
                            if (embeddedStructClassInfo == null)
                            {
                                throw new Exception("Unable to parse metadata for embedded struct.");
                            }
                            payloadFetch = DynamicTraceEventData.PayloadFetch.StructPayloadFetch(offset, embeddedStructClassInfo);
                            break;
                        }
                    default:
                        {
                            throw new NotSupportedException($"{typeCode} is not supported.");
                        }
                }

                // Read the string name of the event payload field.
                fieldNames[fieldIndex] = reader.ReadNullTerminatedUnicodeString();

                // Update the offset into the event for the next payload fetch.
                if (payloadFetch.Size >= DynamicTraceEventData.SPECIAL_SIZES || offset == ushort.MaxValue)
                {
                    offset = ushort.MaxValue;           // Indicate that the offset must be computed at run time.
                }
                else
                {
                    offset += payloadFetch.Size;
                }

                // Save the current payload fetch.
                fieldFetches[fieldIndex] = payloadFetch;
            }

            return new DynamicTraceEventData.PayloadFetchClassInfo()
            {
                FieldNames = fieldNames,
                FieldFetches = fieldFetches
            };
        }

#if SUPPORT_V1_V2
        private void ReadObsoleteEventMetaData(PinnedStreamReader reader, int fileFormatVersionNumber)
        {
            Debug.Assert(fileFormatVersionNumber < 3);

            // Old versions use the stream offset as the MetaData ID, but the reader has advanced to the payload so undo it.  
            MetaDataId = ((int)reader.Current) - EventPipeEventHeader.HeaderSize;

            if (fileFormatVersionNumber == 1)
                _eventRecord->EventHeader.ProviderId = reader.ReadGuid();
            else
            {
                ProviderName = reader.ReadNullTerminatedUnicodeString();
                _eventRecord->EventHeader.ProviderId = GetProviderGuidFromProviderName(ProviderName);
            }

            var eventId = (ushort)reader.ReadInt32();
            _eventRecord->EventHeader.Id = eventId;
            Debug.Assert(_eventRecord->EventHeader.Id == eventId);  // No truncation

            var version = reader.ReadInt32();
            _eventRecord->EventHeader.Version = (byte)version;
            Debug.Assert(_eventRecord->EventHeader.Version == version);  // No truncation

            int metadataLength = reader.ReadInt32();
            if (0 < metadataLength)
                ReadEventMetaData(reader, fileFormatVersionNumber);
        }
#endif

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
            if (string.IsNullOrEmpty(name))
                return Guid.Empty;

            // Legacy GUID lookups (events which existed before the current Guid generation conventions)
            if (name == TplEtwProviderTraceEventParser.ProviderName)
                return TplEtwProviderTraceEventParser.ProviderGuid;
            else if (name == ClrTraceEventParser.ProviderName)
                return ClrTraceEventParser.ProviderGuid;
            else if (name == ClrPrivateTraceEventParser.ProviderName)
                return ClrPrivateTraceEventParser.ProviderGuid;
            else if (name == ClrRundownTraceEventParser.ProviderName)
                return ClrRundownTraceEventParser.ProviderGuid;
            else if (name == ClrStressTraceEventParser.ProviderName)
                return ClrStressTraceEventParser.ProviderGuid;
            else if (name == FrameworkEventSourceTraceEventParser.ProviderName)
                return FrameworkEventSourceTraceEventParser.ProviderGuid;
#if SUPPORT_V1_V2
            else if (name == SampleProfilerTraceEventParser.ProviderName)
                return SampleProfilerTraceEventParser.ProviderGuid;
#endif
            // Hash the name according to current event source naming conventions
            else
                return TraceEventProviders.GetEventSourceGuidFromName(name);
        }

        TraceEventNativeMethods.EVENT_RECORD* _eventRecord;
    }

    /// <summary>
    /// Private utilty class.
    /// 
    /// At the start of every event from an EventPipe is a header that contains
    /// common fields like its size, threadID timestamp etc.  EventPipeEventHeader
    /// is the layout of this.  Events have two variable sized parts: the user
    /// defined fields, and the stack.   EventPipEventHeader knows how to 
    /// decode these pieces (but provides no semantics for it. 
    /// 
    /// It is not a public type, but used in low level parsing of EventPipeEventSource.  
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    unsafe struct EventPipeEventHeader
    {
        private int EventSize;          // Size bytes of this header and the payload and stacks if any.  does NOT incode the size of the EventSize field itself. 
        public int MetaDataId;          // a number identifying the description of this event.  
        public int ThreadId;
        public long TimeStamp;
        public Guid ActivityID;
        public Guid RelatedActivityID;
        public int PayloadSize;         // size in bytes of the user defined payload data. 
        public fixed byte Payload[4];   // Actually of variable size.  4 is used to avoid potential alignment issues.   This 4 also appears in HeaderSize below. 

        public int TotalEventSize => EventSize + sizeof(int);  // Includes the size of the EventSize field itself 

        public bool IsMetadata() => MetaDataId == 0; // 0 means that it's a metadata Id

        /// <summary>
        /// Header Size is defined to be the number of bytes before the Payload bytes.  
        /// </summary>
        static public int HeaderSize => sizeof(EventPipeEventHeader) - 4;

        static public EventPipeEventHeader* HeaderFromPayloadPointer(byte* payloadPtr)
            => (EventPipeEventHeader*)(payloadPtr - HeaderSize);

        static public int StackBytesSize(EventPipeEventHeader* header)
            => *((int*)(&header->Payload[header->PayloadSize]));

        static public byte* StackBytes(EventPipeEventHeader* header)
            => &header->Payload[header->PayloadSize + 4];

        static public byte* PayloadBytes(EventPipeEventHeader* header)
            => &header->Payload[0];
    }
    #endregion
}
