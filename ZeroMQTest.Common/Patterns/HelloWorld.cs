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
    /// <summary>
    /// Hello World using REQ and REP.
    /// </summary>
    public static class HelloWorld
    {
        public static void HWClient(string address = "tcp://127.0.0.1:5555")
        {
            using (var context = ZContext.Create())
            {
                using (var requester = ZSocket.Create(context, ZSocketType.REQ))
                {
                    // Connect
                    requester.Connect(address);

                    for (int n = 0; n < 10; ++n)
                    {
                        string requestText = string.Format("[{0}] Hello", n);
                        LogService.Debug(string.Format("Client: Sending {0}...", requestText));

                        // Send
                        using (var request = new ZFrame(requestText))
                        {
                            requester.Send(request);
                        }

                        // Receive
                        using (var reply = requester.ReceiveFrame())
                        {
                            LogService.Info(string.Format("Client: Received: {0} {1}!", requestText, reply.ReadString()));
                        }
                    }
                }
            }
        }

        public static void HWServer(string address = "tcp://*:5555")
        {
            // Create
            using (var context = new ZContext())
            {
                using (var responder = new ZSocket(context, ZSocketType.REP))
                {
                    LogService.Debug(string.Format("Server: Responder.Bind'ing on {0}", address));

                    // Bind
                    responder.Bind(address);

                    while (true)
                    {
                        // Receive
                        using (ZFrame request = responder.ReceiveFrame())
                        {
                            LogService.Info(string.Format("Server: Received {0}", request.ReadString()));

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
