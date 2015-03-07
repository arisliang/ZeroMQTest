using Lycn.Common.Services;
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
    /// <summary>
    /// Hello World using REQ and REP.
    /// </summary>
    public static class HelloWorld
    {
        public static void HWClient(ZContext context, string address = "tcp://127.0.0.1:5555")
        {
            Contract.Requires(context != null);

            using (var requester = ZSocket.Create(context, ZSocketType.REQ))
            {
                // Connect
                requester.Connect(address);

                for (int n = 0; n < 10; ++n)
                {
                    string requestText = string.Format("[{0}] Hello", n);
                    LogService.Debug(string.Format("{0}: Sending {1}...", Thread.CurrentThread.Name, requestText));

                    // Send
                    using (var request = new ZFrame(requestText))
                    {
                        requester.Send(request);
                    }

                    // Receive
                    using (var reply = requester.ReceiveFrame())
                    {
                        LogService.Info(string.Format("{0}: Received: {1} {2}!", Thread.CurrentThread.Name, requestText, reply.ReadString()));
                    }
                }
            }
        }

        public static void HWServer(ZContext context, string address = "tcp://*:5555")
        {
            Contract.Requires(context != null);

            using (var responder = new ZSocket(context, ZSocketType.REP))
            {
                // Bind or connect via broker
                if (address.Contains('*'))
                {
                    LogService.Debug(string.Format("{0}: Responder binding on {1}", Thread.CurrentThread.Name, address));
                    responder.Bind(address);
                }
                else
                {
                    LogService.Debug(string.Format("{0}: Responder connecting on {1}", Thread.CurrentThread.Name, address));
                    responder.Connect(address);
                }

                string name = "World";
                while (Thread.CurrentThread.IsAlive)
                {
                    // Wait for next request from client
                    using (ZFrame request = responder.ReceiveFrame())
                    {
                        LogService.Info(string.Format("{0}: {1} ", Thread.CurrentThread.Name, request.ReadString()));
                    }

                    // Do some 'work'
                    Thread.Sleep(1);

                    // Send reply back to client
                    LogService.Info(string.Format("{0}: {1}… ", Thread.CurrentThread.Name, name));
                    using (var reply = new ZFrame(name))
                    {
                        responder.Send(reply);
                    }
                }
            }
        }
    }
}
