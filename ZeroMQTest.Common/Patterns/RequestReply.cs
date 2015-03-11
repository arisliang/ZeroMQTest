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
        public static void RRClient(string hwConnectAddress = "tcp://127.0.0.1:5559")
        {
            using (var context = ZContext.Create())
            {
                HelloWorld.HWClient(context, hwConnectAddress);
            }
        }

        public static void RRBroker(string frontEndBindAddress = "tcp://*:5559", string backendBindAddress = "tcp://*:5560")
        {
            using (var context = ZContext.Create())
            {
                using (var frontend = ZSocket.Create(context, ZSocketType.ROUTER))
                {
                    LogService.Debug("{0}: frontend binding on {1}.", Thread.CurrentThread.Name, frontEndBindAddress);
                    frontend.Bind(frontEndBindAddress);

                    using (var backend = ZSocket.Create(context, ZSocketType.DEALER))
                    {
                        LogService.Debug("{0}: backend binding on {1}.", Thread.CurrentThread.Name, backendBindAddress);
                        backend.Bind(backendBindAddress);

                        // Initialize poll set
                        LogService.Debug("{0}: initializing poll set.", Thread.CurrentThread.Name);
                        var poll = ZPollItem.CreateReceiver();

                        // Switch messages between sockets
                        ZError error = null;
                        ZMessage message = null;

                        while (Thread.CurrentThread.IsAlive)
                        {
                            // route frontend request to backend worker
                            if (frontend.PollIn(poll, out message, out error, TimeSpan.FromMilliseconds(AppSetting.POLLMS)))
                            {
                                using (message)
                                {
                                    // Process all parts of the message
                                    LogService.Debug("{0}: Receiving request from frontend.", Thread.CurrentThread.Name);
                                    backend.Send(message);
                                }
                            }
                            else
                            {
                                if (error == ZError.ETERM) return; // Interrupted
                                if (error != ZError.EAGAIN) throw new ZException(error);
                            }

                            // route backend response to frontend client
                            if (backend.PollIn(poll, out message, out error, TimeSpan.FromMilliseconds(AppSetting.POLLMS)))
                            {
                                // Process all parts of the message
                                LogService.Debug("{0}: Receiving response from backend.", Thread.CurrentThread.Name);
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
        }

        public static void RRWorker(string hwConnectAddress = "tcp://127.0.0.1:5560")
        {
            using (var context = ZContext.Create())
            {
                HelloWorld.HWServer(context, hwConnectAddress);
            }
        }
    }
}
