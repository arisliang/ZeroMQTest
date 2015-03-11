using Lycn.Common.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ZeroMQ;

namespace ZeroMQTest.Common.Patterns
{
    public static class NodeCoordination
    {
        public static void SyncPub(int numOfSubscribers, string publisherBindAddress = "tcp://*:5561", string syncserviceBindAddress = "tcp://*:5562")
        {
            using (var context = ZContext.Create())
            {
                using (var publisher = ZSocket.Create(context, ZSocketType.PUB))
                {
                    publisher.SendHighWatermark = 1100000;
                    publisher.Bind(publisherBindAddress);

                    using (var syncservice = ZSocket.Create(context, ZSocketType.REP))
                    {
                        syncservice.Bind(syncserviceBindAddress);

                        // Get synchronization from subscribers
                        int subscribers = numOfSubscribers;
                        do
                        {
                            LogService.Debug("{0}: Waiting for {1} subscriber(s)", Thread.CurrentThread.Name, subscribers);

                            // - wait for synchronization request
                            syncservice.ReceiveFrame();

                            // - send synchronization reply
                            syncservice.Send(new ZFrame());
                        }
                        while (--subscribers > 0);

                        // Now broadcast exactly 20 updates followed by END
                        LogService.Debug("{0}: Broadcasting messages", Thread.CurrentThread.Name);
                        for (int i = 0; i < 20; i++)
                        {
                            LogService.Debug("{0}: Sending {1}...", Thread.CurrentThread.Name, i);
                            publisher.Send(new ZFrame(i));

                            Thread.Sleep(1);
                        }

                        publisher.Send(new ZFrame("END"));
                    }
                }
            }
        }

        public static void SyncSub(string subscriberConnectAddress = "tcp://127.0.0.1:5561", string syncclinetConnectAddress = "tcp://127.0.0.1:5562")
        {
            using (var context = ZContext.Create())
            {
                using (var subscriber = ZSocket.Create(context, ZSocketType.SUB))
                {
                    // First, connect our subscriber socket
                    subscriber.Connect(subscriberConnectAddress);
                    subscriber.SubscribeAll();

                    // 0MQ is so fast, we need to wait a while…
                    Thread.Sleep(1000);

                    using (var syncclient = ZSocket.Create(context, ZSocketType.REQ))
                    {
                        // Second, synchronize with publisher
                        syncclient.Connect(syncclinetConnectAddress);

                        // - send a synchronization request
                        syncclient.Send(new ZFrame());

                        // - wait for synchronization reply
                        syncclient.ReceiveFrame();

                        // Third, get our updates and report how many we got
                        int i = 0;
                        while (true)
                        {
                            using (ZFrame frame = subscriber.ReceiveFrame())
                            {
                                string text = frame.ReadString();
                                if (text == "END")
                                {
                                    LogService.Debug("{0}: Receiving {1}...", Thread.CurrentThread.Name, text);
                                    break;
                                }

                                frame.Position = 0;
                                LogService.Debug("{0}: Receiving {1}...", Thread.CurrentThread.Name, frame.ReadInt32());

                                ++i;
                            }
                        }
                        LogService.Debug("{0}: Received {1} updates.", Thread.CurrentThread.Name, i);
                    }
                }
            }
        }
    }
}
