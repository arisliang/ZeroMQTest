using Lycn.Common.Services;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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
    public static class ParallelTask
    {
        public static void TaskVent(string taskVentBindAddress = "tcp://*:5557", string taskSinkConnectAddress = "tcp://127.0.0.1:5558")
        {
            using (var context = ZContext.Create())
            {
                using (var sender = ZSocket.Create(context, ZSocketType.PUSH))
                {
                    sender.Bind(taskVentBindAddress);

                    using (var sink = ZSocket.Create(context, ZSocketType.PUSH))
                    {
                        sink.Connect(taskSinkConnectAddress);

                        LogService.Debug("Press ENTER when the workers are ready...");
                        Console.ReadKey(true);
                        LogService.Debug("Sending tasks to workers...");

                        // The first message is "0" and signals start of batch.
                        sink.Send(new byte[] { 0x00 }, 0, 1);   // can signal with blank as well, same as work result in worker?

                        var rnd = new Random();

                        // Send 100 tasks
                        int i = 0;
                        long total_msec = 0;    //  Total expected cost in msecs
                        for (; i < 100; ++i)
                        {
                            // Random workload from 1 to 100 msecs
                            int workload = rnd.Next(100) + 1;
                            total_msec += workload;
                            byte[] action = BitConverter.GetBytes(workload);

                            LogService.Debug(string.Format("Vent: Workload {0}", workload));
                            sender.Send(action, 0, action.Length);
                        }

                        LogService.Debug(string.Format("Vent: Total expected cost: {0} ms", total_msec));
                    }
                }
            }
        }

        public static void TaskWork(string taskVentConnectAddress = "tcp://127.0.0.1:5557", string taskSinkConnectAddress = "tcp://127.0.0.1:5558")
        {
            using (var context = ZContext.Create())
            {
                using (var receiver = ZSocket.Create(context, ZSocketType.PULL))
                {
                    receiver.Connect(taskVentConnectAddress);

                    using (var sink = ZSocket.Create(context, ZSocketType.PUSH))
                    {
                        sink.Connect(taskSinkConnectAddress);

                        // Process tasks forever
                        int length = sizeof(int);   // 4
                        while (true)
                        {
                            var replyBytes = new byte[length];
                            receiver.ReceiveBytes(replyBytes, 0, replyBytes.Length);
                            int workload = BitConverter.ToInt32(replyBytes, 0);
                            LogService.Info(string.Format("Worker: {0}.", workload));   // Show progress

                            Thread.Sleep(workload); // Do the work

                            sink.Send(new byte[0], 0, 0);   // Send results to sink
                        }
                    }
                }
            }
        }

        public static void TaskSink(string taskSinkBindAddress = "tcp://*:5558")
        {
            using (var context = ZContext.Create())
            {
                using (var sink = ZSocket.Create(context, ZSocketType.PULL))
                {
                    sink.Bind(taskSinkBindAddress);

                    // Wait for start of batch
                    sink.ReceiveFrame();

                    // Start our clock now.
                    var stopwatch = new Stopwatch();
                    stopwatch.Start();

                    // Process 100 confirmations. Need to know 100?
                    for (int i = 0; i < 100; ++i)
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
                }
            }
        }
    }
}
