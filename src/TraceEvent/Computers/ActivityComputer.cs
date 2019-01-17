// Copyright (c) Microsoft Corporation.  All rights reserved
// This file is best viewed using outline mode (Ctrl-M Ctrl-O)
//
// This program uses code hyperlinks available as part of the HyperAddin Visual Studio plug-in.
// It is available from http://www.codeplex.com/hyperAddin 
// using Microsoft.Diagnostics.Tracing.Parsers;
using Microsoft.Diagnostics.Symbols;
using Microsoft.Diagnostics.Tracing.Etlx;
using Microsoft.Diagnostics.Tracing.Parsers;
using Microsoft.Diagnostics.Tracing.Parsers.Clr;
using Microsoft.Diagnostics.Tracing.Parsers.FrameworkEventSource;
using Microsoft.Diagnostics.Tracing.Parsers.Kernel;
using Microsoft.Diagnostics.Tracing.Parsers.Tpl;
using Microsoft.Diagnostics.Tracing.Stacks;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Address = System.UInt64;

namespace Microsoft.Diagnostics.Tracing
{
    /// <summary>
    /// An ActivityComputer is a state machine that track information about Activities.  In particular, it can
    /// compute a activity aware call stack. (GetCallStack).  
    /// </summary>
    public class ActivityComputer
    {
        /// <summary>
        /// Construct a new ActivityComputer that will process events from 'eventLog' and output activity - aware stacks to 'outputStackSource'. 
        /// </summary>
        public ActivityComputer(TraceLogEventSource source, SymbolReader reader, GCReferenceComputer gcReferenceComputer = null)
        {
            if (gcReferenceComputer == null)
            {
                gcReferenceComputer = new GCReferenceComputer(source);
            }

            m_callStackCache = new CallStackCache();
            m_gcReferenceComputer = gcReferenceComputer;

            m_source = source;
            m_eventLog = source.TraceLog;
            m_symbolReader = reader;
            // m_perActivityStackIndexMaps = new Dictionary<CallStackIndex, StackSourceCallStackIndex>[eventLog.Activities.Count + 1];
            m_threadToCurrentActivity = new TraceActivity[m_eventLog.Threads.Count];

            // Every thread starts out needing auto-start.  Thus the thread activity is really only for the first time.  
            m_threadNeedsToAutoStart = new bool[m_eventLog.Threads.Count];
            for (int i = 0; i < m_threadNeedsToAutoStart.Length; i++)
            {
                m_threadNeedsToAutoStart[i] = true;
            }

            m_rawIDToActivity = new Dictionary<Address, TraceActivity>(64);

            // Allocate the low number indexes for all threads in the system.   
            m_indexToActivity.Count = m_eventLog.Threads.Count;
            m_beginWaits.Count = m_eventLog.Threads.Count;

            TplEtwProviderTraceEventParser tplParser = new TplEtwProviderTraceEventParser(m_source);
            // Normal Tasks. 
            tplParser.TaskScheduledSend += delegate (TaskScheduledArgs data)
            {
                // TODO we are protecting ourselves against a task being scheduled twice (we ignore the second one).   
                // This does happen when you do AwaitTaskContinuationScheduled and then you do a TaskScheduled later.
                var rawScheduledActivityId = GetTPLRawID(data, data.TaskID, IDType.TplScheduledTask);
                TraceActivity activity;
                if (!m_rawIDToActivity.TryGetValue(rawScheduledActivityId, out activity))
                {
                    OnCreated(data, rawScheduledActivityId, TraceActivity.ActivityKind.TaskScheduled);
                }
                else
                {
                    Log.DebugWarn(activity.kind == TraceActivity.ActivityKind.AwaitTaskScheduled, "Two scheduled events on the same Task", data);
                }
            };
            tplParser.TaskExecuteStart += delegate (TaskStartedArgs data) { OnStart(data, GetTPLRawID(data, data.TaskID, IDType.TplScheduledTask)); };
            tplParser.TaskExecuteStop += delegate (TaskCompletedArgs data)
            {
                TraceActivity activity;
                m_rawIDToActivity.TryGetValue(GetTPLRawID(data, data.TaskID, IDType.TplScheduledTask), out activity);
#if false 
                if (!m_rawIDToActivity.TryGetValue(GetTPLRawID(data, data.TaskID, IDType.TplScheduledTask), out activity))
                {
                    // Sadly, TaskCompleted events might happen before the TaskWaitEnd if something is awaiting a real task and these
                    // happen on the same thread.    Detect this and simply ignore the TaskCompleted since it was already stopped. 
                    TraceActivity taskEndActivity;
                    if (m_rawIDToActivity.TryGetValue(GetTPLRawID(data, data.TaskID, IDType.TplContinuation), out taskEndActivity) && taskEndActivity.Thread != null &&
                        taskEndActivity.Thread.ThreadID == data.ThreadID)
                        return;     // We have an active TaskWaitEnd (thus it came first), ignore the TaskCompleted event.  
                }
#endif
                OnStop(data, activity);
            };

            // Async support.    ContinueationScheduled are not like beginWait and endWait pairs, so they use the IsScheduled ID.  
            tplParser.AwaitTaskContinuationScheduledSend += delegate (AwaitTaskContinuationScheduledArgs data)
            {
                OnCreated(data, GetTPLRawID(data, data.ContinuationId, IDType.TplScheduledTask), TraceActivity.ActivityKind.AwaitTaskScheduled);
            };
            tplParser.TaskWaitSend += delegate (TaskWaitSendArgs data)
            {
                TraceActivity createdActivity = OnCreated(data, GetTPLRawID(data, data.TaskID, IDType.TplContinuation),
                    data.Behavior == TaskWaitBehavior.Synchronous ? TraceActivity.ActivityKind.TaskWaitSynchronous : TraceActivity.ActivityKind.TaskWait);
                if (createdActivity == null)
                {
                    return;
                }

                // Remember the first begin on this activity (AwaitUnblock support).  
                int idx = (int)createdActivity.Creator.Index;
                if (m_beginWaits[idx] == null)
                {
                    m_beginWaits[idx] = new List<TraceActivity>(4);
                }

                m_beginWaits[idx].Add(createdActivity);
            };
            // A WaitEnd is like TaskStart (you are starting the next continuation). 
            tplParser.TaskWaitStop += delegate (TaskWaitStopArgs data) { OnStart(data, GetTPLRawID(data, data.TaskID, IDType.TplContinuation), true); };

            // Support for .NET Timer class 
            var fxParser = new FrameworkEventSourceTraceEventParser(m_source);
            fxParser.ThreadTransferSend += delegate (ThreadTransferSendArgs data)
            {

                Address id = GetTimerRawID(data, m_gcReferenceComputer.GetReferenceForGCAddress(data.id));
                OnCreated(data, id, ToActivityKind(data.kind));
            };
            fxParser.ThreadTransferReceive += delegate (ThreadTransferReceiveArgs data)
            {
                Address id = GetTimerRawID(data, m_gcReferenceComputer.GetReferenceForGCAddress(data.id));
                OnStart(data, id);
            };

            // System.Threading.ThreadPool.QueueUserWorkItem support.  
            fxParser.ThreadPoolEnqueueWork += delegate (ThreadPoolEnqueueWorkArgs data)
            {
                OnCreated(data, GetClrRawID(data, data.WorkID), TraceActivity.ActivityKind.ClrThreadPool);
            };
            fxParser.ThreadPoolDequeueWork += delegate (ThreadPoolDequeueWorkArgs data)
            {
                OnStart(data, GetClrRawID(data, data.WorkID));
            };

            // With this event, we should not need the hack below that looks at context switches
            // because we fire this before we go to sleep.  
            m_source.Clr.ThreadPoolWorkerThreadWait += delegate (ThreadPoolWorkerThreadTraceData data)
            {
                AutoRestart(data, data.Thread());
            };

            // .NET Network thread pool support 
            m_source.Clr.ThreadPoolIOEnqueue += delegate (ThreadPoolIOWorkEnqueueTraceData data)
            {
                OnCreated(data, GetClrIORawID(data, data.NativeOverlapped), TraceActivity.ActivityKind.ClrIOThreadPool);
            };
            m_source.Clr.ThreadPoolIOPack += delegate (ThreadPoolIOWorkTraceData data)
            {
                OnCreated(data, GetClrIORawID(data, data.NativeOverlapped), TraceActivity.ActivityKind.ClrIOThreadPool);
            };
            m_source.Clr.ThreadPoolIODequeue += delegate (ThreadPoolIOWorkTraceData data)
            {
                OnStart(data, GetClrIORawID(data, data.NativeOverlapped));
            };

            m_source.Clr.ThreadPoolEnqueue += delegate (ThreadPoolWorkTraceData data)
            {
                OnCreated(data, GetClrRawID(data, data.WorkID), TraceActivity.ActivityKind.ClrThreadPool);
            };

            m_source.Clr.ThreadPoolDequeue += delegate (ThreadPoolWorkTraceData data)
            {
                OnStart(data, GetClrRawID(data, data.WorkID));
            };

            // Unfortunately, the thread pool will go onto other tasks without anything that 
            // says we have transitioned.  Just like cswitches in the thread pool mark this 
            // We will make Start of of the ASP.NET provider also mark an auto-start. 
            // What we really need is to know when TPL activities end, but we don't have that.
            // This will help.   
            m_source.Dynamic.AddCallbackForProviderEvent("Microsoft-Windows-ASPNET", "Request/Start", delegate (TraceEvent data)
            {
                AutoRestart(data, data.Thread());
            });

            // This should not be needed if the thread pool provided proper events.  Basically we want to know
            // when a activity must have ended because we are blocking in the thread pool where we park threads
            // waiting for the 'next thing'.  
            m_source.Kernel.ThreadCSwitch += delegate (CSwitchTraceData data)
            {
                AutoRestartIfNecessary(data);

                // Any time we are blocking (when the thread is switching out) 
                TraceThread thread = m_eventLog.Threads.GetThread(data.OldThreadID, data.TimeStampRelativeMSec);
                if (thread == null)
                {
                    return;
                }

                int threadIndex = (int)thread.ThreadIndex;
                if (m_threadToCurrentActivity.Length <= threadIndex)
                {
                    return;
                }

                TraceActivity activity = m_threadToCurrentActivity[threadIndex];
                if (activity == null)
                {
                    return;
                }

                // If the thread is parked in the thread pool then we know that we should stop this activity (since it clearly 
                // is not running anymore.   
                if (IsThreadParkedInThreadPool(m_eventLog, data.BlockingStack()))
                {
                    int count = 0;
                    while (activity != null)
                    {
                        OnStop(data, activity, thread);
                        TraceActivity newActivity = activity.prevActivityOnThread;
                        count++;
                        if (activity == newActivity || count > 1000)
                        {
                            m_symbolReader.Log.WriteLine("Error: prevActivityOnThread is recursive {0}", activity.Name);
                            break;
                        }
                        activity = newActivity;
                    }

                    // We will make a new activity from the current one then next time we wake up.  
                    m_threadNeedsToAutoStart[threadIndex] = true;
                }
            };
        }

        /* properties of the computer itself */
        /// <summary>
        /// Returns the TraceLog that is associated with the computer (at construction time)
        /// </summary>
        public TraceLog Log { get { return m_eventLog; } }

        /* Callbacks on certain interesting events */
        /// <summary>
        /// Fires when an activity is first created (scheduled).   The activity exists, and has an ID, but has not run yet. 
        /// </summary>
        public event Action<TraceActivity, TraceEvent> Create;
        /// <summary>
        /// First when an activity starts to run (using a thread).  It fires after the start has logically happened.  
        /// so you are logically in the started activity.  
        /// </summary>
        public event Action<TraceActivity, TraceEvent> Start;
        /// <summary>
        /// Fires when the activity ends (no longer using a thread).  It fires just BEFORE the task actually dies 
        /// (that is you ask the activity of the event being passed to 'Stop' it will still give the passed
        /// activity as the answer).   The first TraceActivity is the activity that was stopped, the second
        /// is the activity that exists afer the stop completes.  
        /// </summary>
        public event Action<TraceActivity, TraceEvent> Stop;
        /// <summary>
        /// Like OnStop but gets called AFTER the stop has completed (thus the current thread's activity has been updated)
        /// The activity may be null, which indicates a failure to look up the activity being stopped (and thus the
        /// thread's activity will be set to null).  
        /// </summary>
        public event Action<TraceActivity, TraceEvent, TraceThread> AfterStop;

        /// <summary>
        /// AwaitUnblocks is a specialized form of the 'Start' event that fires when a task starts because
        /// an AWAIT has ended.   The start event also fires on awaits end and comes AFTER the AwaitUnblocks
        /// event has been delivered.    
        /// 
        /// Not every AWAIT end causes a callback.  Because an AWAIT begin happens for every FRAME you only
        /// want a callback for the FIRST task (activity) created by parent of this activity.  This is what
        /// this callback does.  
        /// 
        /// AwaitUnblocks are often treated differently because you want to consider the time between the begin 
        /// (Activity Created) and awaitUnbock to be accounted for as on the critical path, whereas for 'normal' 
        /// tasks you normally don't think that time is interesting.  
        /// </summary>
        public event Action<TraceActivity, TraceEvent> AwaitUnblocks;

        /* Getting activities from other things */
        /// <summary>
        /// Fetches the current activity for 'thread'  at the present time (the current event being dispatched).  
        /// Never returns null because there is always and activity (it may be the thread task).  
        /// This is arguably the main thing that this computer keeps track of.   
        /// </summary>
        public TraceActivity GetCurrentActivity(TraceThread thread)
        {
            int index = (int)thread.ThreadIndex;
            var ret = m_threadToCurrentActivity[index];
            if (ret == null)
            {
                ret = GetActivityRepresentingThread(thread);
                m_threadToCurrentActivity[index] = ret;
            }
            return ret;
        }
        /// <summary>
        /// Gets the default activity for a thread (the activity a thread is doing when the thread starts).  
        /// </summary>
        public TraceActivity GetActivityRepresentingThread(TraceThread thread)
        {
            int index = (int)thread.ThreadIndex;
            var ret = m_indexToActivity[index];     // We pre-allocate a set of activities (one for each thread) so that the index of the activity is the same as the thread index. 
            if (ret == null)
            {
                Debug.Assert(m_beginWaits.Count == m_indexToActivity.Count);
                ret = new TraceActivity((ActivityIndex)index, null, EventIndex.Invalid, CallStackIndex.Invalid,
                                                        thread.startTimeQPC, Address.MaxValue, false, false, TraceActivity.ActivityKind.Initial);
                m_indexToActivity[index] = ret;
                ret.endTimeQPC = thread.endTimeQPC;
                ret.thread = thread;
            }
            return ret;
        }
        /// <summary>
        /// Maps an activity index back to its activity.  
        /// </summary>
        public TraceActivity this[ActivityIndex index] { get { return m_indexToActivity[(int)index]; } }

        /* Getting stacks from activities */
        /// <summary>
        /// Returns a activity-aware call stackIndex associated with'ouputStackSource' for the call stack associated with 'data'.
        /// Such activity-aware call stacks have pseudo-frame every time on thread causes another task to run code (because the  
        /// creator 'caused' the target code).
        /// 
        /// If 'topFrames' is non-null, then this function is called with a Thread and is expected to return a CallStack index that
        /// represents the thread-and-process nodes of the stack.   This allows the returned stack to be have pseudo-frames 
        /// at the root of the stack.  Typically this is used to represent the 'request' or other 'global' context.   If it is not
        /// present the thread and process are used to form these nodes.  
        /// 
        /// This needs to be a function mapping threads to the stack base rather than just the stack base  because in the presence 
        /// of activities the thread at the 'base' whose 'top' you want may not be the one that 'data' started with, so the caller 
        /// needs to be prepared to answer the question about any thread.  
        /// </summary>
        public StackSourceCallStackIndex GetCallStack(MutableTraceEventStackSource outputStackSource, TraceEvent data, Func<TraceThread, StackSourceCallStackIndex> topFrames = null, bool trimEtwFrames = false)
        {
            // OutputStackSource must be derived from the same TraceLog
            Debug.Assert(outputStackSource.TraceLog == m_eventLog);

            m_outputSource = outputStackSource;
            TraceThread thread = data.Thread();
            TraceActivity activity = GetCurrentActivity(thread);
            CallStackIndex callStack = data.CallStackIndex();

            if (trimEtwFrames)
            {
                callStack = TrimETWFrames(callStack);
            }

            m_curEvent = data;
            m_recursion = 0;
            return GetCallStackWithActivityFrames(callStack, activity, topFrames);
        }

        /// <summary>
        /// Returns a StackSource call stack associated with outputStackSource for the activity 'activity'   (that is the call stack at the 
        /// the time this activity was first created.   This stack will have it 'top' defined by topFrames (by default just the thread and process frames)
        /// </summary>
        public StackSourceCallStackIndex GetCallStackForActivity(MutableTraceEventStackSource outputStackSource, TraceActivity activity, Func<TraceThread, StackSourceCallStackIndex> topFrames = null)
        {
            Debug.Assert(outputStackSource.TraceLog == m_eventLog);
            m_outputSource = outputStackSource;

            m_curEvent = null;
            m_recursion = 0;
            return GetCallStackWithActivityFrames(CallStackIndex.Invalid, activity, topFrames);
        }

        /// <summary>
        /// This is not a call stack but rather the chain of ACTIVITIES (tasks), and can be formed even when call stacks   
        /// 
        /// Returns a Stack Source stack associated with outputStackSource where each frame is a task starting with 'activity' and
        /// going back until the activity has no parent (e.g. the Thread's default activity).  
        /// </summary>
        public StackSourceCallStackIndex GetActivityStack(MutableTraceEventStackSource outputStackSource, TraceActivity activity)
        {
            Debug.Assert(activity != null);
            // OutputStackSource must be derived from the same TraceLog
            Debug.Assert(outputStackSource.TraceLog == m_eventLog);

            StackSourceCallStackIndex ret = m_activityStackCache.Get((int)activity.Index);
            if (ret == default(StackSourceCallStackIndex))
            {
                var frameIndex = outputStackSource.Interner.FrameIntern(activity.Name);
                StackSourceCallStackIndex callerFrame;
                var creator = activity.Creator;
                if (creator == null)
                {
                    callerFrame = outputStackSource.GetCallStackForProcess(activity.Thread.Process);
                }
                else
                {
                    callerFrame = GetActivityStack(outputStackSource, creator);
                }

                ret = outputStackSource.Interner.CallStackIntern(frameIndex, callerFrame);

                m_activityStackCache.Set((int)activity.Index, ret);
            }
            return ret;
        }

        /// <summary>
        /// We assume that the top frames are dependent ONLY of the TraceActivity (chunk of a Thread).   If the top-frames
        /// change WITHIN the current TraceActivity (which they easily could), you are responsible for clearing
        /// the cache whenever the top-frames change (within that activity). 
        /// things that var
        /// </summary>
        public void ClearCache()
        {
            m_callStackCache.ClearStackEntries();
        }

        // This is the amount of task recursion you allow (one task calling another), AFTER
        // you eliminate simple 'tail recursive' cases (which are pretty common and long).
        // Note that ETW cuts of stacks at 200 frames.   Thus 500 Tasks (each of which may have
        // several frames) should be plenty (and that does not even include the tail recursion)
        // If you make this too big the display will get slow.  Values over 2000 might start
        // causing stack overflows.   
        const ushort MaxTaskRecursion = 30; // TODO FIX NOW, 10 for debugging, 100-500 is reasonable.  

        #region Private
        // When we stop because we notice we are in the threadpool, then we need to auto-start it
        // when it wakes up.  This does that logic. 
        private void AutoRestartIfNecessary(TraceEvent data)
        {
            var thread = data.Thread();
            if (thread == null)
                return;

            int threadIndex = (int)thread.ThreadIndex;
            if (m_threadNeedsToAutoStart[threadIndex])
            {
                m_threadNeedsToAutoStart[threadIndex] = false;
                AutoRestart(data, thread);
            }
        }

        private void AutoRestart(TraceEvent data, TraceThread thread)
        {
            if (thread == null)
                return;

            // TODO: ideally we just call OnCreated, and OnStarted. 
            // Can't remember why I did not do this... 

            // Create a new activity 
            TraceActivity autoStartActivity = new TraceActivity((ActivityIndex)m_indexToActivity.Count, null, data.EventIndex, CallStackIndex.Invalid,
                data.TimeStampQPC, Address.MaxValue, false, false, TraceActivity.ActivityKind.Implied);

            var create = Create;
            if (create != null)
            {
                create(autoStartActivity, data);
            }

            m_indexToActivity.Add(autoStartActivity);
            m_beginWaits.Add(null);

            // Start it 
            autoStartActivity.prevActivityOnThread = m_threadToCurrentActivity[(int)thread.ThreadIndex];
            m_threadToCurrentActivity[(int)thread.ThreadIndex] = autoStartActivity;
            autoStartActivity.startTimeQPC = data.TimeStampQPC;
            autoStartActivity.thread = thread;

            var start = Start;
            if (start != null)
            {
                start(autoStartActivity, data);
            }
        }

        /// <summary>
        /// Returns true if the call stack is in the thread pool parked (not running user code)  
        /// This means that the thread CAN'T be running an active activity and we can kill it.  
        /// </summary>
        internal static bool IsThreadParkedInThreadPool(TraceLog eventLog, CallStackIndex callStackIndex)
        {
            // Empty stacks are not parked in the thread pool.  
            if (callStackIndex == CallStackIndex.Invalid)
            {
                return false;
            }

            int count = 0;
            string prevFilePath = null;
            bool seenThreadPoolDll = false;
            for (; ; )
            {
                CodeAddressIndex codeAddressIndex = eventLog.CallStacks.CodeAddressIndex(callStackIndex);
                TraceModuleFile moduleFile = eventLog.CodeAddresses.ModuleFile(codeAddressIndex);
                if (moduleFile == null)
                {
                    return false;
                }

                // If NGEN images are on the path, you are not parked 
                var filePath = moduleFile.FilePath;

                // If you are outside OS code or CLR code.  We err on the side of returning false.  
                // There are a number of cases (coreclr, desktop)   
                if (filePath.Length < 1)
                {
                    return false;
                }

                // To be parked, EVERY Frame has to be in the OS or CLR.dll.  
                // First we check if they are in the in the system (which is either \windows\System32 or \windows\SysWow64)
                var startIdx = (filePath[0] == '\\') ? 0 : 2;       // WE allow there to be no drive latter
                if (String.Compare(filePath, startIdx, @"\windows\sys", 0, 12, StringComparison.OrdinalIgnoreCase) != 0)
                {
                    // However we DO allow CLR (or CoreCLR) to be there, which we allow to be anywhere.  
                    // If we directly transition from the CLR to the kernel we assume it is an APC and not
                    // a parking wait and return false.   If you don't do this ThreadPoolEnqueue events get
                    // stopped before they get going.  
                    if (filePath.EndsWith("clr.dll", StringComparison.OrdinalIgnoreCase) || filePath.EndsWith("mscorwks.dll", StringComparison.OrdinalIgnoreCase))
                    {
                        seenThreadPoolDll = true;
                        if (prevFilePath != null && prevFilePath.EndsWith("ntoskrnl.exe", StringComparison.OrdinalIgnoreCase))
                        {
                            return false;
                        }
                    }
                    else if (!filePath.EndsWith("webengine4.dll", StringComparison.OrdinalIgnoreCase))
                    {
                        return false;       // Otherwise it is not parked (which includes everything in the GAC, NIC, mscorlib ... and user code elsewhere)
                    }
                }
                else
                {
                    // We consider the w3tp to be a thread pool DLL as well.  
                    if (filePath.EndsWith("w3tp.dll", StringComparison.OrdinalIgnoreCase))
                    {
                        seenThreadPoolDll = true;
                    }
                }
                prevFilePath = filePath;

                // Parking does not take more than 30 frames.  (this is generous)
                count++;
                if (count > 30)
                {
                    return false;
                }

                callStackIndex = eventLog.CallStacks.Caller(callStackIndex);
                if (callStackIndex == CallStackIndex.Invalid)
                {
                    // We only return true if we have seen the CLR module.   I have seen stacks
                    // where you only see allocation in the WOW DLLs.   I don't know how this happens
                    // but this protects against it.  If we don't do this the ThreadPoolEnqueue events 
                    // don't work (becuase they get terminated prematurely by this stack.  
                    // +ntdll!_LdrpInitialize       
                    //| +wow64!Wow64LdrpInitialize       
                    //| +wow64!RunCpuSimulation         
                    //| +wow64cpu!ServiceNoTurbo          
                    //| +wow64!Wow64SystemServiceEx      
                    //| +wow64!whNtOpenKeyEx           
                    //| +wow64!Wow64NtOpenKey       
                    //| +ntdll!RtlFreeHeap          
                    //| +ntdll!RtlpFreeHeap        
                    //| +ntoskrnl!KiDpcInterrupt    
                    if (!seenThreadPoolDll)
                    {
                        return false;
                    }

                    // We have to avoid broken frames.   Thus the top most frame must be in nt.dll
                    var ret = (moduleFile.FilePath.EndsWith("ntdll.dll", StringComparison.OrdinalIgnoreCase));
                    return ret;
                }
            }
        }

        /// <summary>
        /// This cache remembers Activity * CallStackIndex pairs and the resulting StackSourceCallStackIndex
        /// It also remembers some intermediate results like the LogicalCreator and LogicalStackAtCreation
        /// because these don't get invalidated by 'ClearStackEntries' and are pretty important to avoid
        /// VERY long processing in activities that tail-call a lot (which does happen). 
        /// </summary>
        private class CallStackCache : MutableTraceEventStackSource.CallStackMap
        {
            public CallStackCache()
            {
                CurrentActivityIndex = ActivityIndex.Invalid;       // You have to set this before calling get or put.  
                Debug.Assert(((CacheSize - 1) & CacheSize) == 0);   // Enforce that the Cache Size is a power of two.  
                _entries = new CacheEntry[CacheSize];
                for (int i = 0; i < _entries.Length; i++)
                {
                    _entries[i] = new CacheEntry() { _fromActivityIndex = ActivityIndex.Invalid };
                }

                _discardEntry = new CacheEntry();
            }

            #region CallStackMap interface support 
            /// <summary>
            /// Remembers the current Activity for 'Get' and 'Put' operations.   Needs to be set before Get or Put is called.  
            /// Note that we can't add this to the Get and Put operations because they are part of the CallStackMap interface.  
            /// </summary>
            public ActivityIndex CurrentActivityIndex;

            /// <summary>
            /// Gets the cache entry for the CurrnetActivityIndex with the call stack 'fromStackIndex'  returns Invalid if
            /// there is no entry.   
            /// 
            /// This is not passed the CurrentActivityIndex, so it can implement the CallStackMap interface
            /// </summary>
            public StackSourceCallStackIndex Get(CallStackIndex fromStackIndex)
            {
                Debug.Assert(CurrentActivityIndex != ActivityIndex.Invalid);
                CacheEntry entry = GetEntry(fromStackIndex, CurrentActivityIndex);
                if (entry != null)
                    return entry._toTaskIndex;
                return StackSourceCallStackIndex.Invalid;
            }

            public void Put(CallStackIndex fromStackIndex, StackSourceCallStackIndex toStackIndex)
            {
                Debug.Assert(CurrentActivityIndex != ActivityIndex.Invalid);
                CacheEntry entry = GetEntryForUpdate(fromStackIndex, CurrentActivityIndex);
                entry._toTaskIndex = toStackIndex;
            }

            #endregion 

            // For debugging only, it is expensive
            public int NumEntries
            {
                get
                {
                    int ret = 0;
                    for (int i = 0; i < _entries.Length; i++)
                    {
                        if (_entries[i]._fromActivityIndex != ActivityIndex.Invalid)
                        {
                            ret++;
                        }
                    }

                    return ret;
                }
            }

            /// <summary>
            /// Clears the 'final stack entry (but not the logical creator information) in the stack.
            /// This is appropriate when the topStacks information changes (e.g. when a start or stop
            /// happens in the start-stop view)
            /// </summary>
            public void ClearStackEntries()
            {
                for (int i = 0; i < _entries.Length; i++)
                {
                    ref CacheEntry entry = ref _entries[i];
                    entry._toTaskIndex = StackSourceCallStackIndex.Invalid;
                    entry._deathAge = 0;
                }
            }

            /// <summary>
            /// Returns the cached information associated the stack represented by 'fromStackIndex' and 'activityIndex'.
            /// Returns null if not found.
            /// </summary>
            internal CacheEntry GetEntry(CallStackIndex fromStackIndex, ActivityIndex activityIndex)
            {
                Debug.Assert(activityIndex != ActivityIndex.Invalid);

                _gets++;
                if ((_gets & (GetsPerClockTick - 1)) == 0)
                    _clock++;
                int hash = ((int)activityIndex + (int)fromStackIndex + ((int)fromStackIndex << 3)) & CacheMask;
                CacheEntry entry = _entries[hash];
                if (entry._fromActivityIndex == activityIndex && entry._fromStackIndex == fromStackIndex)
                {
                    entry._deathAge |= 32;               // this has proven useful keep it around longer as well as resetting the last hit.  
                    entry._lastHitCount = _clock;
                    return entry;
                }
                return null;
            }

            /// <summary>
            /// Looks for a entry for the information associated with the stack represented by fromStackIndex and activityIndex.
            /// The entry can then be filled in with the information.  This routine never returns null.  
            /// </summary>
            internal CacheEntry GetEntryForUpdate(CallStackIndex fromStackIndex, ActivityIndex activityIndex)
            {
                _gets++;
                if ((_gets & (GetsPerClockTick - 1)) == 0)
                    _clock++;
                int hash = ((int)activityIndex + (int)fromStackIndex + ((int)fromStackIndex << 3)) & CacheMask;
                CacheEntry entry = _entries[hash];

                // TODO FIX NOW DISABLE CACHE.  
                entry = _discardEntry;
                entry._initialized = false;
                entry._toTaskIndex = StackSourceCallStackIndex.Invalid;
                entry._taskDepth = 0;
                entry._logicalCreator = null;
                entry._logicalStackAtCreation = CallStackIndex.Invalid;
                entry._stackStop = CallStackIndex.Invalid;
                entry._fromActivityIndex = activityIndex;
                entry._fromStackIndex = fromStackIndex;
                return entry;

                if (entry._fromStackIndex != fromStackIndex || entry._fromActivityIndex != activityIndex)
                {
                    if (entry._deathAge < ((ushort)(_clock - entry._lastHitCount))) // if we have not used it recently, pitch it and reuse the entry
                    {
                        entry._lastHitCount = _clock;
                        entry._deathAge = 0;                         // By default we evict aggressively (next hit)
                        if ((_clock & 0x4) == 0)                     // Every 4th entry we let live longer
                        {
                            entry._deathAge = 4;                     // this will last several call stacks 
                            if ((_clock & 0x1F) == 0)                // Every 32nd entry we let longer still.  
                                entry._deathAge = 32;
                        }
                    }
                    else
                    {
                        entry = _discardEntry;
                    }

                    entry._initialized = false;
                    entry._toTaskIndex = StackSourceCallStackIndex.Invalid;
                    entry._taskDepth = 0;
                    entry._logicalCreator = null;
                    entry._logicalStackAtCreation = CallStackIndex.Invalid;
                    entry._stackStop = CallStackIndex.Invalid;
                    entry._fromActivityIndex = activityIndex;
                    entry._fromStackIndex = fromStackIndex;
                }
                else
                    entry._deathAge |= 32;                           // this has proven useful keep it around longer as well as resetting the last hit.  

                return entry;
            }

            #region private
            private const int CacheSize = 4096 * 4;                 // Must be a power of 2
            private const int CacheMask = CacheSize - 1;
            private const ushort GetsPerClockTick = 32;             // The size of a typical stack, on the small side.   Must be a power of 2.             

            internal class CacheEntry
            {
                // The first two entries are a logical stack 
                internal CallStackIndex _fromStackIndex;         // The stack starts here
                internal ActivityIndex _fromActivityIndex;       // After FromStack, the rest of the stack is where this activity was created.  

                // These two entries represent the stack at creation time (same convention)
                // However this stack also removes recursion.  Thus if one activity spawns itself (common) we have 
                // eliminated that part of the stack (since it can be arbitrarily long). 
                // Not this does not depend on the 'topFrames' and thus caches perfectly (never needs to be discarded)
                internal TraceActivity _logicalCreator;
                internal CallStackIndex _logicalStackAtCreation;
                internal CallStackIndex _stackStop;

                // This is the final StackSourceStack for the original stack (which was in pieces).  Note
                // however that this depends on the 'topFrames' and thus might be invalidated if the top-frames
                // change (e.g. when start or stop events happen).   Thus this is more fragile.  
                internal StackSourceCallStackIndex _toTaskIndex;
                internal ushort _taskDepth;                  // This is the depth in Task recursions.  We wish to limit this to MaxTaskRecursion so the viewer does not slow down.   
                internal bool _initialized;

                // internal variables to decide who to evict when we need to add a new entry.  
                internal ushort _lastHitCount;               // used to decide who to evict.  
                internal ushort _deathAge;                   // if you are older than this die.  
            }

            private ushort _gets;                            // This is advanced every Get operation 
            private ushort _clock;                           // Counts how many times we use the cache
            private CacheEntry[] _entries;
            internal CacheEntry _discardEntry;              // returned by GetEntryForUpdate when there is no free cache entry.
            #endregion
        }

        private static bool NeedsImplicitCompletion(TraceActivity.ActivityKind kind) { return (((int)kind & 32) != 0); }

        private static TraceActivity.ActivityKind ToActivityKind(ThreadTransferKind threadTransferKind)
        {
            Debug.Assert(threadTransferKind <= ThreadTransferKind.WinRT);
            if (threadTransferKind == ThreadTransferKind.ManagedTimers)
            {
                return TraceActivity.ActivityKind.FxTimer;
            }

            return (TraceActivity.ActivityKind)((int)TraceActivity.ActivityKind.FxTransfer + (int)threadTransferKind);
        }

        /// <summary>
        /// Creation handles ANY creation of a task.  
        /// </summary>
        private TraceActivity OnCreated(TraceEvent data, Address rawScheduledActivityId, TraceActivity.ActivityKind kind)
        {
            Debug.Assert(m_beginWaits.Count == m_indexToActivity.Count);
            // TODO FIX NOW think about the timers case.  
            Debug.Assert(!m_rawIDToActivity.ContainsKey(rawScheduledActivityId) ||
                m_rawIDToActivity[rawScheduledActivityId].kind == TraceActivity.ActivityKind.FxTimer ||
                m_rawIDToActivity[rawScheduledActivityId].kind == TraceActivity.ActivityKind.ClrIOThreadPool ||
                m_rawIDToActivity[rawScheduledActivityId].kind == TraceActivity.ActivityKind.ClrThreadPool ||
                m_rawIDToActivity[rawScheduledActivityId].kind == TraceActivity.ActivityKind.FxAsyncIO);

            TraceThread thread = data.Thread();
            if (thread == null)
            {
                return null;
            }

            TraceActivity creator = GetCurrentActivity(thread);

            TraceActivity created = new TraceActivity((ActivityIndex)m_indexToActivity.Count, creator, data.EventIndex, data.CallStackIndex(),
                data.TimeStampQPC, rawScheduledActivityId, false, false, kind);

            m_indexToActivity.Add(created);
            m_beginWaits.Add(null);

            m_rawIDToActivity[rawScheduledActivityId] = created;

            // Invoke user callback if present
            var create = Create;
            if (create != null)
            {
                create(created, data);
            }

            return created;
        }

        private void OnStart(TraceEvent data, Address rawActivityId, bool isAwaitEnd = false)
        {
            // EndWaits don't have a Stop associated with them.  It is assumed that there can only be one outstanding one so
            // so if we see a TaskWaitEnd AND the previous one is a TaskWaitEnd we will auto-stop it.    
            TraceThread thread = data.Thread();
            if (thread == null)
            {
                return;
            }

            TraceActivity existingActivity = m_threadToCurrentActivity[(int)thread.ThreadIndex];
            if (existingActivity != null && isAwaitEnd && NeedsImplicitCompletion(existingActivity.kind))
            {
                OnStop(data, existingActivity, thread);
            }

            // Get the activity.  
            TraceActivity activity;
            m_rawIDToActivity.TryGetValue(rawActivityId, out activity);

            // If we can't find the activity we drop the event
            if (activity == null)
            {
                var kind = GetTypeFromRawID(rawActivityId);
                if (kind != IDType.Timer)        // Because timers might be set long ago (and be recurring), don't bother warning (although we do drop it...)
                {
                    m_symbolReader.Log.WriteLine("Warning: An activity was started that was not scheduled at {0:n3}  in process {1} of kind {2}",
                        data.TimeStampRelativeMSec, thread.Process.Name, kind);
                }

                return;
            }
            if (activity.prevActivityOnThread != null)
            {
                // IO ThreadPool allows multiple dequeues.  
                if (GetTypeFromRawID(rawActivityId) != IDType.IOThreadPool)
                {
                    m_symbolReader.Log.WriteLine("Error: Starting an activity twice! {0:n3} ", data.TimeStampRelativeMSec);
                }

                return;
            }

            // Indicate that this thread is executing this activity by pushing it on the stack of activities.  
            activity.prevActivityOnThread = m_threadToCurrentActivity[(int)thread.ThreadIndex];
            m_threadToCurrentActivity[(int)thread.ThreadIndex] = activity;

            // Mark the activity as started.  
            activity.startTimeQPC = data.TimeStampQPC;
            activity.thread = thread;

            // Invoke the AwaitUnblocks user callback if present
            var creator = activity.Creator;
            if (creator != null)
            {
                if (isAwaitEnd)
                {
                    int creatorIdx = (int)creator.Index;
                    List<TraceActivity> beginWaits = m_beginWaits[creatorIdx];
                    if (beginWaits != null)
                    {

                        // We only consider the first BeginWait that was created by same activity to be truly blocking 
                        // If there are any begin waits that this one matches, use that.   
                        var index = beginWaits.IndexOf(activity);
                        if (0 <= index)     // Is this activity present
                        {
                            var awaitUnblocks = AwaitUnblocks;
                            if (awaitUnblocks != null)
                            {
                                awaitUnblocks(activity, data);
                            }

                            if (0 < index)
                            {
                                // We don't expect this.  We hope that TaskBegin and TaskEnd are done in stack order.  Warn me.  
                                m_symbolReader.Log.WriteLine("Activity Tracking: Warning: BeginWait-EndWait do not follow stack protocol  at {0:n3}, discarding {1}",
                                    data.TimeStampRelativeMSec, index);
                            }
                            beginWaits.Clear();
                        }
                    }
                }
            }

            // Send the user defined start event. 
            var start = Start;
            if (start != null)
            {
                start(activity, data);
            }

            // Synchronous waitEnds auto-complete.  
            if (activity.kind == TraceActivity.ActivityKind.TaskWaitSynchronous)
            {
                OnStop(data, activity, thread);
            }
        }

        /// <summary>
        /// Activity can be null, which means we could not figure out the activity we are stopping.  
        /// </summary>
        private void OnStop(TraceEvent data, TraceActivity activity, TraceThread thread = null)
        {
            if (thread == null)
            {
                thread = data.Thread();
                if (thread == null)
                {
                    return;
                }
            }

            // Invoke user callback if present.  We do this BEFORE the activity stops as that is the most useful.   
            var stop = Stop;
            if (stop != null && activity != null)
            {
                if (activity.endTimeQPC == 0)      // we have an unstopped activity.  
                {
                    stop(activity, data);
                }
            }

            // Stop all activities that are on the stack until we get to this one.   
            var cur = m_threadToCurrentActivity[(int)thread.ThreadIndex];
            for (; ; )
            {
                if (cur == null)
                {
                    m_symbolReader.Log.WriteLine("Warning: stopping an activity that was not started at {0:n3}", data.TimeStampRelativeMSec);
                    m_threadToCurrentActivity[(int)thread.ThreadIndex] = null;      // setting to null means the set to thread activity.  
                    break;
                }
                Debug.Assert(cur.Thread == thread);
                if (cur == activity)
                {
                    m_threadToCurrentActivity[(int)thread.ThreadIndex] = cur.prevActivityOnThread;
                    break;
                }
                if (cur.kind == TraceActivity.ActivityKind.Initial)
                {
                    break;
                }
                if (activity != null)
                {
                    if (!NeedsImplicitCompletion(cur.kind))
                    {
                        m_symbolReader.Log.WriteLine("Warning: task start-stop pairs do not match up {0:n3} stopping activity that started at {1:n3}",
                            data.TimeStampRelativeMSec, activity.StartTimeRelativeMSec);
                    }
                    OnStop(data, cur, thread);
                    Debug.Assert(cur != m_threadToCurrentActivity[(int)thread.ThreadIndex]);        // OnStop updated m_threadToCurrentActivity.  
                    cur = m_threadToCurrentActivity[(int)thread.ThreadIndex];
                }
                else
                {
                    cur = cur.prevActivityOnThread;
                }
            }

            var afterStop = AfterStop;
            if (afterStop != null)
            {
                afterStop(activity, data, thread);
            }

            // If we don't have an activity we can't do more.  
            if (activity == null)
            {
                return;
            }

            // Mark the activity as stopped
            if (activity.endTimeQPC == 0)
            {
                activity.endTimeQPC = data.TimeStampQPC;
            }
            else
            {
                Log.DebugWarn(activity.IsThreadActivity, "Activity " + activity.Name + " stopping when already stopped!", data);
            }

            // Remove it from the map if it is not multi-trigger (we are done with it).  
            if (!activity.MultiTrigger)
            {
                m_rawIDToActivity.Remove(activity.rawID);
            }
        }

        private enum IDType
        {
            None = 0,
            TplContinuation = 1,
            TplScheduledTask = 2,
            Timer = 3,
            IOThreadPool = 4,
            ThreadPool = 5,
        }

        /// <summary>
        /// Get a trace wide ID for a TPL event.   TPL tasks might be 'Scheduled' in the sense
        /// that it might run independently on another thread.  Tasks that do 'BeginWait and 'EndWait'
        /// are not scheduled.  The same ID might have both operating simultaneously (if you wait
        /// on a scheduled task).  Thus you need an independent ID for both.  
        /// </summary>
        private static Address GetTPLRawID(TraceEvent data, int taskID, IDType idType)
        {
            Debug.Assert(idType == IDType.TplContinuation || idType == IDType.TplScheduledTask);
            uint highBits = (((uint)data.ProcessID) << 8) + (((uint)idType) << 28);
            // TODO FIX NOW add appDomain ID
            return (((Address)highBits) << 32) + (uint)taskID;
        }

        private static Address GetTimerRawID(TraceEvent data, GCReferenceID gcReference)
        {
            uint highBits = (((uint)data.ProcessID) << 8) + (((uint)IDType.Timer) << 28);
            return (((Address)highBits) << 32) + (uint)gcReference;
        }

        private static Address GetClrIORawID(TraceEvent data, Address nativeOverlapped)
        {
            uint highBits = (((uint)data.ProcessID) << 8) + (((uint)IDType.IOThreadPool) << 28);
            return (((Address)highBits) << 32) + nativeOverlapped;          // TODO this is NOT absolutely guaranteed not to collide.   
        }

        private static Address GetClrRawID(TraceEvent data, Address workId)
        {
            uint highBits = (((uint)data.ProcessID) << 8) + (((uint)IDType.ThreadPool) << 28);
            return (((Address)highBits) << 32) + workId;          // TODO this is NOT absolutely guaranteed not to collide.   
        }

        private static IDType GetTypeFromRawID(Address rawID)
        {
            return (IDType)(0xF & (rawID >> (60)));
        }

#if false 
        /// <summary>
        /// Bit of a hack.  Currently CLR thread pool does not have complete events to indicate a thread 
        /// pool item is complete.  Because of this they may be extended too far.   We use the fact that 
        /// we have a call stack that is ONLY in the thread pool as a way of heuristically finding the end.  
        /// </summary>
        static bool ThreadOnlyInThreadPool(CallStackIndex callStack, TraceCallStacks callStacks)
        {
            var codeAddresses = callStacks.CodeAddresses;
            bool brokenStack = true;
            while (callStack != CallStackIndex.Invalid)
            {
                var codeAddrIdx = callStacks.CodeAddressIndex(callStack);
                var module = codeAddresses.ModuleFile(codeAddrIdx);
                if (module == null)
                    break;
                var moduleName = module.Name;
                if (!moduleName.StartsWith("wow", StringComparison.OrdinalIgnoreCase) &&
                    !moduleName.StartsWith("kernel", StringComparison.OrdinalIgnoreCase) &&
                    string.Compare(moduleName, "ntdll", StringComparison.OrdinalIgnoreCase) != 0 &&
                    string.Compare(moduleName, "w3tp", StringComparison.OrdinalIgnoreCase) != 0 &&
                    string.Compare(moduleName, "clr", StringComparison.OrdinalIgnoreCase) != 0 &&
                    string.Compare(moduleName, "mscorwks", StringComparison.OrdinalIgnoreCase) != 0)
                {
                    if (string.Compare(moduleName, "ntoskrnl", StringComparison.OrdinalIgnoreCase) != 0)
                        return false;
                }
                else
                    brokenStack = false;
                callStack = callStacks.Caller(callStack);
            }
            return !brokenStack;
        }
#endif

        /// <summary>
        /// if 'activity' does not have a creator (it is top-level), then return baseStack (near execution) followed by 'top' representing the thread-process frames.
        /// 
        /// otherwise, find the fragment of 'baseStack' up to the point to enters the threadpool (the user code) and splice it to the stack of the creator
        /// of the activity and return that.  (thus returning your full user-stack).  
        /// 
        /// You have to set the m_recursion field to 0 before calling this routine.   You can inspect  m_retTaskDepth after the call returns.  
        /// </summary>
        private StackSourceCallStackIndex GetCallStackWithActivityFrames(CallStackIndex stackStart, TraceActivity activity, Func<TraceThread, StackSourceCallStackIndex> topFrames)
        {
            CallStackCache.CacheEntry entry = m_callStackCache.GetEntryForUpdate(stackStart, activity.Index);

            // We have a cached final result!  Yeah, we are done!
            if (entry._toTaskIndex != StackSourceCallStackIndex.Invalid)
            {
                // We put things in the cache if TaskDepth < MaxTaskRecursion and it does NOT have an overflow frame OR MaxTaskRecursion <= TaskDepth and it DOES have an overflow frame.
                // Given OUR m_recursion variable see if what is cached matches what we actually want.  
                if (entry._taskDepth + m_recursion < MaxTaskRecursion ||     // We DON'T want an overflow frame (and the cache also does not have one)
                    MaxTaskRecursion <= entry._taskDepth)                    // We DO want an overflow frame (and the cache has one)
                {
                    // Trace.WriteLine("Hit cache for " + activity.Index + " " + startStack);
                    m_retTaskDepth = entry._taskDepth;
                    m_overflowFrame = MaxTaskRecursion <= entry._taskDepth;
                    return entry._toTaskIndex;
                }
                // else
                //Trace.WriteLine("Cache missed because of recursion frame " + activity.Index + " " + stackStart);
            }

            string error = null;
            if (!entry._initialized) // If we did not find info, initialize it.  
            {
                error = GetLogicalCreatorCallStack(stackStart, activity, ref entry._logicalCreator, ref entry._logicalStackAtCreation, ref entry._stackStop);
                entry._initialized = true;
            }

            if (entry._logicalCreator != null)
            {
                CallStackIndex stackStop = entry._stackStop;
                StackSourceCallStackIndex fullCreationStack;
                m_recursion++;
                if (MaxTaskRecursion <= m_recursion)
                {
                    // Give up and simply show the stacks from the activities throw the thread and process node.    
                    fullCreationStack = GetTopStacksForThread(activity.Thread, topFrames);

                    // Add a frame that says we had to limit recursion and this is not our real creator.  
                    fullCreationStack = m_outputSource.Interner.CallStackIntern(m_outputSource.Interner.FrameIntern("ERROR_EXCESSIVE_TASK_RECURSION"), fullCreationStack);
                    entry._taskDepth = m_retTaskDepth = 0;
                    m_overflowFrame = true;
                    //Trace.WriteLine("LOGGING EXCESSIVE_TASK_RECURSION frame " + activity.Index + " " + stackStart);
                    entry = null; // We never want to cache this entry.  
                }
                else
                {
                    // Call ourselves recursively to get the tail 'close to threadStart' part of the stack.   
                    fullCreationStack = GetCallStackWithActivityFrames(entry._logicalStackAtCreation, entry._logicalCreator, topFrames);

                    m_retTaskDepth++;

                    // We only add the frame to the if 
                    //     TaskDepth < MaxTaskRecursion and we did NOT add an EXCESSIVE TASK RECURSION frame or
                    //   MaxTaskRecursion <= TaskDepth and we did add an EXCESSIVE TASK RECURSION frame
                    bool myReturnNeedsOverflowFrame = (MaxTaskRecursion <= m_retTaskDepth);
                    if (m_overflowFrame == myReturnNeedsOverflowFrame)
                        entry._taskDepth = m_retTaskDepth;
                    else
                    {
                        entry = null;       // This means don't set the cache.  
                        //Trace.WriteLine("Avoiding caching of stacks associated with " + activity.Index + " " + stackStart);
                    }

                    // Add a frame that shows that we are starting a task 
                    fullCreationStack = m_outputSource.Interner.CallStackIntern(m_outputSource.Interner.FrameIntern("STARTING TASK"), fullCreationStack);
                }
                // and take the region between start and stops where we enter the thread pool and concatenate it to fullCreationStack.  
                var ret = SpliceStack(stackStart, stackStop, fullCreationStack, entry);
                return ret;
            }
            else
            {
                // This is the top most activity, there is no splicing, just to the 'normal' stack processing.  
                m_callStackCache.CurrentActivityIndex = activity.Index;
                entry._toTaskIndex = m_outputSource.GetCallStack(stackStart, GetTopStacksForThread(activity.Thread, topFrames, error), m_callStackCache);
                entry._taskDepth = m_retTaskDepth = 0;
                m_overflowFrame = true;
                return entry._toTaskIndex;
            }
        }

        private StackSourceCallStackIndex GetTopStacksForThread(TraceThread thread, Func<TraceThread, StackSourceCallStackIndex> topFrames, string errorFrame = null)
        {
            StackSourceCallStackIndex ret;
            if (topFrames != null)
                ret = topFrames(thread);
            else
                ret = m_outputSource.GetCallStackForThread(thread);

            if (errorFrame != null)
                ret = m_outputSource.Interner.CallStackIntern(m_outputSource.Interner.FrameIntern(errorFrame), ret);
            return ret;
        }

        /// <summary>
        /// Given the stack represented by 'startStack' and 'activity' see if there is a creator activity and if return three pieces of information
        ///  * logicalCreator -> The activity that created  'activity' 
        ///  * logicalStackAtCreation -> The stack fragment where 'activity' was created.  This does not include the ETW logging frames.   
        ///  * stackStop -> the transition from 'startStack' where it enters the thread pool 
        ///  
        /// Note that this is the 'logical' creator because we skip recursion.   If the current activity is the same as the creator activity 
        /// then we skip that creator and look at its creator.  Thus we remove recursion.  
        /// 
        /// It returns a string that indicates any error that happened, or null if it is successful.  
        /// </summary> 
        private string GetLogicalCreatorCallStack(CallStackIndex startStack, TraceActivity activity, ref TraceActivity logicalCreator, ref CallStackIndex logicalStackAtCreation, ref CallStackIndex stackStopRet)
        {
            CallStackIndex stackToCache = CallStackIndex.Invalid;
            TraceActivity activityToCache = null;
            CallStackIndex stackStopToCache = CallStackIndex.Invalid;

            CallStackIndex stackStop;
            string error = null;

            Debug.Assert(activity != null);
            for (int recCount = 0; ; recCount++)
            {
                // Set logical Creator 
                logicalCreator = activity.Creator;
                if (logicalCreator == null)
                    return null;

                // We place an upper bound on how many recursive task we scan in case there
                // is bad data that has loops in the creator tree.   
                if (recCount >= 10000)
                {
                    // If we change this so that logicalCreator != null, we need to change the caller
                    logicalCreator = null;
                    error = "ERROR_EXCESSIVE_TASK_SIMPLE_RECURSION";
                    break;
                }

                //Trace.WriteLine("GetLogicalCreatorStack " + activity.Index + " " + startStack + " recCount " + recCount + " depth " + m_recursion);

                // Set the startStack return value.   
                if (startStack != CallStackIndex.Invalid)
                {
                    stackStop = FindThreadPoolTransition(startStack);
                    if (stackStop == CallStackIndex.Invalid)
                    {
                        // We could not find a thread pool transition.  This is probably a broken stack.  
                        // Today we complete give up on stack stitching.   We may wish to revisit this.  
                        // If we change this so that logicalCreator != null, we need to change the caller
                        logicalCreator = null;
                        error = "ERROR_NO_THREADPOOL_TRANSITION";
                        break;
                    }
                }
                else
                    stackStop = CallStackIndex.Invalid;

                // The stackStopRet value is only for the original stackStart variable.   
                // Thus only set it the first time through.  
                if (recCount == 0)
                    stackStopRet = stackStop;

                // We cache the 4th entry so that if we have sibling activities 
                // that they don't need to do the whole recursion chain again.  
                // We need the final values (which we don't have yet) so we just
                // remember what the inputs were and add these to the cache before
                // we return.  
                if (recCount == 3)
                {
                    stackToCache = startStack;
                    activityToCache = activity;
                    stackStopToCache = stackStop;
                }


                // Set logicalStack At creation time.  
                logicalStackAtCreation = TrimETWFrames(activity.CreationCallStackIndex);

                // If we are not recursive we are done. 
                if (logicalCreator.Creator == null || !IsRecursiveTask(startStack, stackStop, logicalStackAtCreation))
                    break;

                // Skip this creator it looks just like us (recursion).    
                startStack = logicalStackAtCreation;
                activity = logicalCreator;

                // See if we have cached the answer.  
                var entry = m_callStackCache.GetEntry(startStack, activity.Index);
                if (entry != null && entry._initialized)
                {
                    //Trace.WriteLine("Found IN CACHE " + activity.Index + " " + startStack + " recCount " + recCount);
                    logicalCreator = entry._logicalCreator;
                    logicalStackAtCreation = entry._logicalStackAtCreation;
                    stackStop = entry._stackStop;
                    break;
                }
            }

            // Also cache a entry that is 4 away.  This makes it pretty unlikely that we have to traverse large changes multiple times. 
            if (activityToCache != null)
            {
                var addEntry = m_callStackCache.GetEntryForUpdate(stackToCache, activityToCache.Index);
                //Trace.WriteLine("ADDING " + activityToCache.Index + " " + stackToCache);
                addEntry._stackStop = stackStopToCache;
                addEntry._logicalCreator = logicalCreator;
                addEntry._logicalStackAtCreation = logicalStackAtCreation;
                addEntry._initialized = true;
            }
            return error;
        }

        /* Support functions for GetCallStack */
        /// <summary>
        /// Trims off frames that call ETW logic and return.   If the pattern is not matched, we return  callStackIndex
        /// </summary>
        private CallStackIndex TrimETWFrames(CallStackIndex callStackIndex)
        {
            if (m_methodFlags == null)
            {
                ResolveWellKnownSymbols();
            }

            CallStackIndex ret = callStackIndex;        // iF we don't see the TplEtwProvider.TaskScheduled event just return everything.   
            bool seenTaskScheduled = false;
            while (callStackIndex != CallStackIndex.Invalid)
            {
                CodeAddressIndex codeAddressIndex = m_eventLog.CallStacks.CodeAddressIndex(callStackIndex);
                MethodIndex methodIndex = m_eventLog.CallStacks.CodeAddresses.MethodIndex(codeAddressIndex);

                callStackIndex = m_eventLog.CallStacks.Caller(callStackIndex);

                // TODO FIX NOW fix if you don't have symbols 
                if (((uint)methodIndex < (uint)m_methodFlags.Length))
                {
                    MethodFlags flags = m_methodFlags[(int)methodIndex];
                    if (seenTaskScheduled)
                    {
                        if ((flags & MethodFlags.TaskScheduleHelper) == 0)  // We have already TplEtwProvider.TaskScheduled.  If this is not a helper, we are done.  
                        {
                            break;
                        }

                        ret = callStackIndex;                               // Eliminate the helper frame as well.  
                    }
                    else if ((flags & (MethodFlags.TaskSchedule | MethodFlags.TaskWaitEnd)) != 0)       // We see TplEtwProvider.TaskScheduled, (or TaskWaitEnd) eliminate at least this, but see if we can eliminate helpers above.  
                    {
                        seenTaskScheduled = true;
                        ret = callStackIndex;
                    }
                }
            }
            return ret;
        }

        /// <summary>
        /// If the stack from 'startStack' (closest to execution) through 'stopStack' is the same as 'parentStack'
        /// (and that the next frame on 'parentStack is a ThreadPoolTransition.  
        /// 
        /// Basically this indicates that the stack chunk associated with 'curStack' is identical to its parent
        /// activity (thus it is recursive), (and can be omitted).  
        /// </summary>
        private bool IsRecursiveTask(CallStackIndex curStack, CallStackIndex stopStack, CallStackIndex parentStack)
        {
            return false; // TODO FIX NOW FOR TESTING.
            while (true)
            {
                if (curStack == CallStackIndex.Invalid)
                    return false;
                if (parentStack == CallStackIndex.Invalid)
                    return false;
                if (curStack == stopStack)
                    break;

                CodeAddressIndex parentCodeAddressIdx = m_eventLog.CallStacks.CodeAddressIndex(parentStack);
                CodeAddressIndex curCodeAddressIdx = m_eventLog.CallStacks.CodeAddressIndex(curStack);
                if (curCodeAddressIdx == CodeAddressIndex.Invalid || parentCodeAddressIdx == CodeAddressIndex.Invalid)
                    return false;

                if (parentCodeAddressIdx != curCodeAddressIdx)
                {
                    if (m_eventLog.CodeAddresses.MethodIndex(parentCodeAddressIdx) != m_eventLog.CodeAddresses.MethodIndex(parentCodeAddressIdx))
                        return false;
                    if (m_eventLog.CodeAddresses.ModuleFileIndex(parentCodeAddressIdx) != m_eventLog.CodeAddresses.ModuleFileIndex(parentCodeAddressIdx))
                        return false;
                }

                parentStack = m_eventLog.CallStacks.Caller(parentStack);
                curStack = m_eventLog.CallStacks.Caller(curStack);
            }

            Debug.Assert(parentStack != CallStackIndex.Invalid);
            Debug.Assert(curStack != CallStackIndex.Invalid);
            return FindThreadPoolTransition(parentStack, 1) == parentStack;
        }

        /// <summary>
        /// Create a stack which is executing at 'stackStart' and finds the region until 'stackStio', appending that (in order) to 'stackStart'.  
        /// 'entry' is a cache entry and is optional.  It should be the cache entry for 'stackStart'.  If it is non-null we search there, and 
        /// also update the entry with the CallStack->StackSource mapping
        /// </summary>
        private StackSourceCallStackIndex SpliceStack(CallStackIndex stackStart, CallStackIndex stackStop, StackSourceCallStackIndex baseStack, CallStackCache.CacheEntry entry)
        {
            if (stackStart == CallStackIndex.Invalid || stackStart == stackStop)
                return baseStack;

            if (entry != null && entry._toTaskIndex != StackSourceCallStackIndex.Invalid)
                return entry._toTaskIndex;

            var codeAddress = m_eventLog.CallStacks.CodeAddressIndex(stackStart);
            var caller = m_eventLog.CallStacks.Caller(stackStart);
            CallStackCache.CacheEntry nextEntry = null;
            if (entry != null)
            {
                nextEntry = m_callStackCache.GetEntryForUpdate(caller, entry._fromActivityIndex);
                nextEntry._taskDepth = entry._taskDepth;
            }
            var callerStack = SpliceStack(caller, stackStop, baseStack, nextEntry);
            var frameIdx = m_outputSource.GetFrameIndex(codeAddress);
            var ret = m_outputSource.Interner.CallStackIntern(frameIdx, callerStack);

            if (entry != null)
                entry._toTaskIndex = ret;
            return ret;
        }

        /// <summary>
        /// Returns the point in 'callStackIndex' where the CLR thread pool transitions from 
        /// a thread pool worker to the work being done by the threadpool.  
        /// 
        /// Basically we find the closest to execution (furthest from thread-start) call to a 'Run' method
        /// that shows we are running an independent task.  
        /// </summary>
        private CallStackIndex FindThreadPoolTransition(CallStackIndex callStackIndex, int maxSearch = int.MaxValue)
        {
            if (m_methodFlags == null)
            {
                ResolveWellKnownSymbols();
            }

            CodeAddressIndex codeAddressIndex = CodeAddressIndex.Invalid;
            CallStackIndex ret = CallStackIndex.Invalid;
            CallStackIndex curFrame = callStackIndex;
            int searchCount = 0;
            while (curFrame != CallStackIndex.Invalid && searchCount < maxSearch)
            {
                codeAddressIndex = m_eventLog.CallStacks.CodeAddressIndex(curFrame);
                MethodIndex methodIndex = m_eventLog.CodeAddresses.MethodIndex(codeAddressIndex);

                // TODO FIX NOW fix if you don't have symbols 
                if ((uint)methodIndex < (uint)m_methodFlags.Length)
                {
                    var flags = m_methodFlags[(int)methodIndex];
                    if ((flags & MethodFlags.TaskRun) != 0)
                    {
                        if (ret == CallStackIndex.Invalid)
                        {
                            ret = curFrame;
                        }

                        return ret;
                    }
                    else if ((flags & MethodFlags.TaskRunHelper) != 0)
                    {
                        ret = curFrame;
                    }
                }
                else
                {
                    ret = CallStackIndex.Invalid;
                }

                curFrame = m_eventLog.CallStacks.Caller(curFrame);
                searchCount++;
            }
            // This happens after the of end of the task or on broken stacks.  
            return CallStackIndex.Invalid;
        }

        /// <summary>
        /// Used by TrimETWFrames and FindThreadPoolTransition to find particular frame names and place the information in 'm_methodFlags'
        /// </summary>
        private void ResolveWellKnownSymbols()
        {
            Debug.Assert(m_methodFlags == null);

            StringWriter sw = new StringWriter();

            foreach (TraceModuleFile moduleFile in m_eventLog.ModuleFiles)
            {
                if (moduleFile.Name.StartsWith("mscorlib.ni", StringComparison.OrdinalIgnoreCase) || moduleFile.Name.StartsWith("system.private.corelib", StringComparison.OrdinalIgnoreCase))
                {
                    // We can skip V2.0 runtimes (we may have more than one because 64 and 32 bit)  
                    if (!moduleFile.FilePath.Contains("NativeImages_v2"))
                    {
                        m_eventLog.CodeAddresses.LookupSymbolsForModule(m_symbolReader, moduleFile);
                    }
                }
            }

            bool foundThreadingAPIs = false;
            TraceMethods methods = m_eventLog.CodeAddresses.Methods;
            m_methodFlags = new MethodFlags[methods.Count];
            for (MethodIndex methodIndex = 0; methodIndex < (MethodIndex)methods.Count; methodIndex++)
            {
                TraceModuleFile moduleFile = m_eventLog.ModuleFiles[methods.MethodModuleFileIndex(methodIndex)];
                if (moduleFile == null)
                {
                    continue;
                }

                if (moduleFile.Name.StartsWith("mscorlib", StringComparison.OrdinalIgnoreCase) || moduleFile.Name.IndexOf("system.private.corelib", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    string name = methods.FullMethodName(methodIndex);
                    if (name.StartsWith("System.Threading.ExecutionContext.Run") ||
                        name.StartsWith("System.Threading.Tasks.AwaitTaskContinuation.Run") ||
                        name.StartsWith("System.Threading.Tasks.Task.Execute"))
                    {
                        m_methodFlags[(int)methodIndex] |= MethodFlags.TaskRun;
                        foundThreadingAPIs = true;
                    }
                    else if (name.Contains("System.Threading.Tasks.Task") && name.Contains(".InnerInvoke"))
                    {
                        m_methodFlags[(int)methodIndex] |= MethodFlags.TaskRunHelper;
                    }
                    else if (name.StartsWith("System.Threading.Tasks.TplEtwProvider.TaskScheduled") || name.StartsWith("System.Threading.Tasks.TplEtwProvider.TaskWaitBegin("))
                    {
                        m_methodFlags[(int)methodIndex] |= MethodFlags.TaskSchedule;
                    }
                    else if (name.StartsWith("System.Threading.Tasks.TplEtwProvider.TaskWaitEnd"))
                    {
                        m_methodFlags[(int)methodIndex] |= MethodFlags.TaskWaitEnd;
                    }
                    else if ((name.StartsWith("System.Runtime.CompilerServices.AsyncTaskMethodBuilder") && name.Contains(".AwaitUnsafeOnCompleted")) ||
                             name.StartsWith("System.Threading.Tasks.Task.ScheduleAndStart") ||
                             (name.StartsWith("System.Runtime.CompilerServices") && name.Contains("TaskAwaiter") &&
                                (name.Contains("OnCompleted") || name.Contains("OutputWaitEtwEvents"))))
                    {
                        m_methodFlags[(int)methodIndex] |= MethodFlags.TaskScheduleHelper;
                    }

                    m_methodFlags[(int)methodIndex] |= MethodFlags.Mscorlib;
                }
            }
            if (!foundThreadingAPIs)
            {
                // It can happen but it's not a critical failure. Log the issue instead of throwing.
                // TODO: Looking for better ways to inform the users when this happens.
                m_symbolReader.Log.WriteLine("Error: Could not resolve symbols for Task library (mscorlib), task stacks will not work.");
            }
        }

        /// <summary>
        /// We look for various well known methods inside the Task library.   This array maps method indexes 
        /// and returns a bitvector of 'kinds' of methods (Run, Schedule, ScheduleHelper).  
        /// </summary>
        private MethodFlags[] m_methodFlags;
        [Flags]
        private enum MethodFlags : byte
        {
            TaskRun = 1,                  // This is a method that marks a frame that runs a task (frame toward threadStart are irrelevant)
            TaskRunHelper = 2,            // This method if 'below' (away from thread root) from a TackRun should also be removed.  
            TaskSchedule = 4,             // This is a method that marks the scheduling of a task (frames toward execution are irrelevant)
            TaskScheduleHelper = 8,       // This method if 'above' (toward thread root), from a TaskSchedule should also be removed.  
            TaskWaitEnd = 16,
            Mscorlib = 32                 // and mscorlib method
        }

        private bool[] m_threadNeedsToAutoStart;
        private TraceActivity[] m_threadToCurrentActivity;                      // Remembers the current activity for each thread in the system.  
        private Dictionary<Address, TraceActivity> m_rawIDToActivity;           // Maps tasks (or other raw IDs) to their activity.  
        private GrowableArray<TraceActivity> m_indexToActivity;                 // Maps activity Indexes to activities.  

        // Cache for GetActivityStack
        private GrowableArray<StackSourceCallStackIndex> m_activityStackCache;

        // When you AWAIT a task, you actually make a task per frame.   Since these always overlap in time 
        // You only want only one of to have AWAIT time.  We choose the first WaitBegin for this.  
        private GrowableArray<List<TraceActivity>> m_beginWaits;                // Maps activity index to all WaitBegin on that activity.  (used for AwaitUnblock)

        private TraceEventDispatcher m_source;
        private TraceLog m_eventLog;
        private SymbolReader m_symbolReader;
        private MutableTraceEventStackSource m_outputSource;
        private TraceEvent m_curEvent;                                  // used for diagnostics, like to remove it...
        private GCReferenceComputer m_gcReferenceComputer;

        /* This is set before GetCallStackWithActivityFrames is called */
        private ushort m_recursion;                                     // The number of times GetCallStackWithActivityFrames is called recursively.
        private bool m_overflowFrame;                                   // We inserted a overflow frame

        /* These are set before GetCallStackWithActivityFrames returns */
        private ushort m_retTaskDepth;                                  // The task depth of the last call to  GetCallStackWithActivityFrames

        private CallStackCache m_callStackCache;                        // Speeds things up by remembering previously computed entries. 

        #endregion
    }

#if UNUSED
    // TODO FIX NOW use or remove.  
    /// <summary>
    /// Remembers the mapping between threads and activities for all time.  Basically it support 'GetActivity' which takes
    /// a thread and a time and returns the activity.  
    /// </summary>
    class ActivityMap
    {
        public ActivityMap(ActivityComputer computer)
        {
            m_computer = computer;
            m_ActivityMap = new GrowableArray<ActivityEntry>[computer.Log.Threads.Count];

            m_computer.Create += delegate(TraceActivity activity, TraceEvent data)
            {
                Debug.Assert(activity.Thread == data.Thread());
                InsertActivityForThread(ref m_ActivityMap[(int)activity.Thread.ThreadIndex], activity);
            };
        }
        public TraceActivity GetActivity(TraceEvent data)
        {
            return GetActivity(data.Thread(), data.TimeStampRelativeMSec);
        }
        public TraceActivity GetActivity(TraceThread thread, double timeStampRelativeMSec)
        {
            return GetActivityForThread(ref m_ActivityMap[(int)thread.ThreadIndex], timeStampRelativeMSec, thread);
        }

    #region private

        private void InsertActivityForThread(ref GrowableArray<ActivityEntry> threadTable, TraceActivity activity)
        {
            threadTable.Add(new ActivityEntry() { Activity = activity, TimeStampRelativeMSec = activity.StartTimeRelativeMSec });
        }
        private TraceActivity GetActivityForThread(ref GrowableArray<ActivityEntry> threadTable, double timeStampRelativeMSec, TraceThread thread)
        {
            int index;
            threadTable.BinarySearch<double>(timeStampRelativeMSec, out index, (time, elem) => time.CompareTo(elem.TimeStampRelativeMSec));
            if (index < 0)
                return m_computer.GetActivityRepresentingThread(thread);
            return threadTable[index].Activity;
        }

        struct ActivityEntry
        {
            public TraceActivity Activity;
            public double TimeStampRelativeMSec;
        }

        GrowableArray<ActivityEntry>[] m_ActivityMap;
        ActivityComputer m_computer;
    #endregion
    }
#endif
}


