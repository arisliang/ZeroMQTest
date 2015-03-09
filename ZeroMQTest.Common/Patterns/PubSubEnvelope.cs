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
    public static class PubSubEnvelope
    {
        public static void PSEnvPub(string publisherBindAddress = "tcp://*:5563")
        {
            using (var context = ZContext.Create())
            {
                using (var publisher = ZSocket.Create(context, ZSocketType.PUB))
                {
                    publisher.Linger = TimeSpan.Zero;
                    publisher.Bind(publisherBindAddress);

                    while (true)
                    {
                        // Write two messages, each with an envelope and content
                        using (var message = new ZMessage())
                        {
                            message.Add(new ZFrame("A"));
                            message.Add(new ZFrame("We don't want to see this"));
                            publisher.Send(message);
                        }
                        using (var message = new ZMessage())
                        {
                            message.Add(new ZFrame("B"));
                            message.Add(new ZFrame("We would like to see this"));
                            publisher.Send(message);
                        }

                        Thread.Sleep(1000);
                    }
                }
            }
        }

        public static void PSEnvSub(string subscriberConnectAddress = "tcp://127.0.0.1:5563")
        {
            using (var context = ZContext.Create())
            {
                using (var subscriber = ZSocket.Create(context, ZSocketType.SUB))
                {
                    subscriber.Connect(subscriberConnectAddress);
                    subscriber.Subscribe("B");

                    while (true)
                    {
                        using (var message = subscriber.ReceiveMessage())
                        {
                            // Read envelope with address
                            string address = message[0].ReadString();

                            // Read message contents
                            string contents = message[1].ReadString();

                            LogService.Info(string.Format("[{0}]: {1} {2}", Thread.CurrentThread.Name, address, contents));
                        }
                    }
                }
            }
        }
    }
}
