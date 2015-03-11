using Lycn.Common.Services;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ZeroMQ;

namespace ZeroMQTest.Common.Patterns
{
    /// <summary>
    /// Parallel Task using PULL and PUSH
    /// </summary>
    public static class ParallelTaskWithKill
    {
        static int _NumberOfTasks = 100;

        public static void TaskVent(ZContext context, string taskVentBindAddress = "tcp://*:5557", string taskSinkConnectAddress = "tcp://127.0.0.1:5558")
        {
            Contract.Requires(context != null);

            using (var sender = ZSocket.Create(context, ZSocketType.PUSH))
            {
                sender.Bind(taskVentBindAddress);

                using (var sink = ZSocket.Create(context, ZSocketType.PUSH))
                {
                    sink.Connect(taskSinkConnectAddress);
                    sink.Linger = new TimeSpan(0, 0, 0);

                    LogService.Debug("Press ENTER when the workers are ready...");
                    Console.ReadKey(true);
                    LogService.Debug("Sending tasks to workers...");

                    // The first message is "0" and signals start of batch.
                    sink.Send(new byte[] { 0x00 }, 0, 1);   // can signal with blank as well, same as work result in worker?

                    var rnd = new Random();

                    // Send 100 tasks
                    int i = 0;
                    long total_msec = 0;    //  Total expected cost in msecs
                    for (; i < _NumberOfTasks; ++i)    // 100 magic number?
                    {
                        // Random workload from 1 to 100 msecs
                        int workload = rnd.Next(100) + 1;
                        total_msec += workload;
                        byte[] action = BitConverter.GetBytes(workload);

                        LogService.Debug("Vent: Workload {0}", workload);
                        sender.Send(action, 0, action.Length);
                    }

                    LogService.Debug("Vent: Total expected cost: {0} ms", total_msec);
                }
            }
        }

        public static void TaskWork(ZContext context, string taskVentConnectAddress = "tcp://127.0.0.1:5557",
            string taskSinkConnectAddress = "tcp://127.0.0.1:5558", string taskSinkControllerConnectAddress = "tcp://127.0.0.1:5559")
        {
            Contract.Requires(context != null);

            using (var receiver = ZSocket.Create(context, ZSocketType.PULL))
            {
                receiver.Connect(taskVentConnectAddress);

                using (var sink = ZSocket.Create(context, ZSocketType.PUSH))
                {
                    sink.Connect(taskSinkConnectAddress);

                    using (var controller = ZSocket.Create(context, ZSocketType.SUB))
                    {
                        controller.Connect(taskSinkControllerConnectAddress);
                        controller.SubscribeAll();

                        var poll = ZPollItem.CreateReceiver();

                        ZError error;
                        ZMessage message;

                        // Process tasks forever
                        while (Thread.CurrentThread.IsAlive)
                        {
                            if (receiver.PollIn(poll, out message, out error, TimeSpan.FromMilliseconds(AppSetting.POLLMS)))
                            {
                                int workload = message[0].ReadInt32();
                                LogService.Info("{0}: {1}.", Thread.CurrentThread.Name, workload);   // Show progress
                                Thread.Sleep(workload); // Do the work
                                sink.Send(new byte[0], 0, 0);   // Send results to sink
                            }

                            // Any waiting controller command acts as 'KILL'
                            if (controller.PollIn(poll, out message, out error, TimeSpan.FromMilliseconds(AppSetting.POLLMS)))
                            {
                                break;  // Exit loop
                            }
                        }
                    }
                }
            }
        }

        public static void TaskSink(ZContext context, string taskSinkBindAddress = "tcp://*:5558", string taskSinkControllerBindAddress = "tcp://*:5559")
        {
            Contract.Requires(context != null);

            using (var sink = ZSocket.Create(context, ZSocketType.PULL))
            {
                sink.Bind(taskSinkBindAddress);

                // Wait for start of batch
                sink.ReceiveFrame();

                // Start our clock now.
                var stopwatch = new Stopwatch();
                stopwatch.Start();

                for (int i = 0; i < _NumberOfTasks; ++i)
                {
                    sink.ReceiveFrame();

                    if ((i / 10) * 10 == i)
                    {
                        LogService.Debug(":");
                    }
                    else
                    {
                        LogService.Debug(".");
                    }
                }

                // Calculate and report duration of batch
                stopwatch.Stop();
                LogService.Info(string.Format("Total elapsed time: {0} ms", stopwatch.ElapsedMilliseconds));

                using (var controller = ZSocket.Create(context, ZSocketType.PUB))
                {
                    controller.Bind("tcp://*:5559");

                    // Send kill signal to workers
                    controller.Send(new ZFrame("KILL"));
                }
            }
        }
    }
}
