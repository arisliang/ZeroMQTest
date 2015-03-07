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
    public static class RequestReply
    {
        static int POLLMS = 64;

        public static void RRClient(ZContext context, string hwConnectAddress = "tcp://127.0.0.1:5559")
        {
            Contract.Requires(context != null);

            HelloWorld.HWClient(context, hwConnectAddress);
        }

        public static void RRBroker(ZContext context, string frontEndBindAddress = "tcp://*:5559", string backendBindAddress = "tcp://*:5560")
        {
            Contract.Requires(context != null);

            var frontendTest = ZSocket.Create(context, ZSocketType.ROUTER);

            using (var frontend = ZSocket.Create(context, ZSocketType.ROUTER))
            {
                LogService.Debug(string.Format("{0}: frontend binding on {1}.", Thread.CurrentThread.Name, frontEndBindAddress));
                frontend.Bind(frontEndBindAddress);

                using (var backend = ZSocket.Create(context, ZSocketType.DEALER))
                {
                    LogService.Debug(string.Format("{0}: backend binding on {1}.", Thread.CurrentThread.Name, backendBindAddress));
                    backend.Bind(backendBindAddress);

                    // Initialize poll set
                    var poll = ZPollItem.CreateReceiver();

                    // Switch messages between sockets
                    ZError error = null;
                    ZMessage message = null;
                    while (Thread.CurrentThread.IsAlive)
                    {
                        // route frontend request to backend worker
                        if (frontend.PollIn(poll, out message, out error, TimeSpan.FromMilliseconds(POLLMS)))
                        {
                            using (message)
                            {
                                // Process all parts of the message
                                LogService.Debug(string.Format("{0}: Receiving request from frontend.", Thread.CurrentThread.Name));
                                backend.Send(message);
                            }
                        }
                        else
                        {
                            if (error == ZError.ETERM) return; // Interrupted
                            if (error != ZError.EAGAIN) throw new ZException(error);
                        }

                        // route backend response to frontend client
                        if (backend.PollIn(poll, out message, out error, TimeSpan.FromMilliseconds(POLLMS)))
                        {
                            // Process all parts of the message
                            LogService.Debug(string.Format("{0}: Receiving response from backend.", Thread.CurrentThread.Name));
                            frontend.Send(message);
                        }
                        else
                        {
                            if (error == ZError.ETERM) return; // Interrupted
                            if (error != ZError.EAGAIN) throw new ZException(error);
                        }
                    }
                }
            }
        }

        public static void RRWorker(ZContext context, string hwConnectAddress = "tcp://127.0.0.1:5560")
        {
            Contract.Requires(context != null);

            HelloWorld.HWServer(context, hwConnectAddress);
        }
    }
}
