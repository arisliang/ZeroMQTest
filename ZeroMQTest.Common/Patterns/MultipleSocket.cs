using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ZeroMQ;

namespace ZeroMQTest.Common.Patterns
{
    public static class MultipleSocketReader
    {
        public static void MSReader(ZContext context,
            string taskVentConnectAddress = "tcp://127.0.0.1:5557",
            string wuConnectAddress = "tcp://127.0.0.1:5556")
        {
            Contract.Requires(context != null);

            using (var receiver = ZSocket.Create(context, ZSocketType.PULL))
            {
                // Connect to task ventilator
                receiver.Connect(taskVentConnectAddress);

                using (var subscriber = ZSocket.Create(context, ZSocketType.SUB))
                {
                    // Connect to weather server
                    subscriber.Connect(wuConnectAddress);
                    subscriber.SetOption(ZSocketOption.SUBSCRIBE, "10001 ");

                    // Process messages from both sockets
                    // We prioritize traffic from the task ventilator by excution ordering.
                    ZError error = null;
                    ZFrame frame = null;

                    while (Thread.CurrentThread.IsAlive)
                    {
                        if (null != (frame = receiver.ReceiveFrame(ZSocketFlags.DontWait, out error)))
                        {
                            // process task
                        }
                        else
                        {
                            if (error == ZError.ETERM) return;  // Interrupted
                            if (error != ZError.EAGAIN) throw new ZException(error);
                        }

                        if (null != (frame = subscriber.ReceiveFrame(ZSocketFlags.DontWait, out error)))
                        {
                            // Process weather update
                        }
                        else
                        {
                            if (error == ZError.ETERM) return;  // Interrupted
                            if (error != ZError.EAGAIN) throw new ZException(error);
                        }

                        // No activity, so sleep for 1 msec
                        Thread.Sleep(1);
                    }
                }
            }
        }
    }

    public static class MultipleSocketPoller
    {
        public static void MSPoller(ZContext context,
            string taskVentConnectAddress = "tcp://127.0.0.1:5557",
            string wuConnectAddress = "tcp://127.0.0.1:5556")
        {
            using (var receiver = ZSocket.Create(context, ZSocketType.PULL))
            {
                // Connect to task ventilator
                receiver.Connect(taskVentConnectAddress);

                using (var subscriber = ZSocket.Create(context, ZSocketType.SUB))
                {
                    // Connect to weather server
                    subscriber.Connect(wuConnectAddress);
                    subscriber.SetOption(ZSocketOption.SUBSCRIBE, "10001 ");

                    var poll = ZPollItem.CreateReceiver();

                    // Process messages from both sockets
                    ZError error = null;
                    ZMessage msg = null;

                    while (Thread.CurrentThread.IsAlive)
                    {
                        if (receiver.PollIn(poll, out msg, out error, TimeSpan.FromMilliseconds(AppSetting.POLLMS)))
                        {
                            // Process task
                        }
                        else
                        {
                            if (error == ZError.ETERM) return;    // Interrupted
                            if (error != ZError.EAGAIN) throw new ZException(error);
                        }

                        if (subscriber.PollIn(poll, out msg, out error, TimeSpan.FromMilliseconds(AppSetting.POLLMS)))
                        {
                            // Process weather update
                        }
                        else
                        {
                            if (error == ZError.ETERM) return;    // Interrupted
                            if (error != ZError.EAGAIN) throw new ZException(error);
                        }
                    }
                }
            }
        }
    }
}
