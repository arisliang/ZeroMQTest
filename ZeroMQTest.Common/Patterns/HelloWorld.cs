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
    public static class HelloWorld
    {
        public static void HWClient()
        {
            using (var context = ZContext.Create())
            {
                using (var requester = ZSocket.Create(context, ZSocketType.REQ))
                {
                    // Connect
                    requester.Connect("tcp://127.0.0.1:5555");

                    for (int n = 0; n < 10; ++n)
                    {
                        string requestText = string.Format("[{0}] Hello", n);
                        LogService.Info(string.Format("Sending {0}...", requestText));

                        // Send
                        using (var request = new ZFrame(requestText))
                        {
                            requester.Send(request);
                        }

                        // Receive
                        using (var reply = requester.ReceiveFrame())
                        {
                            LogService.Info(string.Format(" Received: {0} {1}!", requestText, reply.ReadString()));
                        }
                    }
                }
            }
        }

        public static void HWServer()
        {
            // Create
            using (var context = new ZContext())
            {
                using (var responder = new ZSocket(context, ZSocketType.REP))
                {
                    // Bind
                    responder.Bind("tcp://*:5555");

                    while (true)
                    {
                        // Receive
                        using (ZFrame request = responder.ReceiveFrame())
                        {
                            Console.WriteLine("Received {0}", request.ReadString());

                            // Do some work
                            Thread.Sleep(1);

                            // Send
                            string name = "World";
                            responder.Send(new ZFrame(name));
                        }
                    }
                }
            }
        }
    }
}
