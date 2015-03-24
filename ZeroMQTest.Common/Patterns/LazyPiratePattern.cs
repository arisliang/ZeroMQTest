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
    public static class LazyPiratePattern
    {
        static TimeSpan LPClient_RequestTimeout = TimeSpan.FromMilliseconds(2000);
        static int LPClient_RequestRetries = 3;

        public static void LazyPirateServer(string responderBindAddress = "tcp://*:5555")
        {
            using (var context = ZContext.Create())
            {
                using (var responder = ZSocket.Create(context, ZSocketType.REP))
                {
                    responder.Bind(responderBindAddress);

                    ZError error = null;
                    int cycles = 0;
                    var rnd = new Random();

                    ZMessage incoming = null;
                    while (true)
                    {
                        if ((incoming = responder.ReceiveMessage(out error)) == null)
                        {
                            if (error == ZError.ETERM) return;  // Interrupted
                            throw new ZException(error);
                        }
                        using (incoming)
                        {
                            cycles++;

                            // Simulate various problems, after a few cycles
                            if (cycles > 16 && rnd.Next(16) == 0)
                            {
                                LogService.Info("{0}: simulating a crash", Thread.CurrentThread.Name);
                                break;
                            }
                            else if (cycles > 4 && rnd.Next(4) == 0)
                            {
                                LogService.Warn("{0}: simulating CPU overload", Thread.CurrentThread.Name);
                                Thread.Sleep(1000);
                            }

                            LogService.Debug("{0}: normal request ({1})", Thread.CurrentThread.Name, incoming[0].ReadInt32());
                            Thread.Sleep(AppSetting.WAITINGMS); // Do some heavy work

                            responder.Send(incoming);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Helper function that returns a new configured socket
        /// connected to the Lazy Pirate queue
        /// </summary>
        /// <param name="context"></param>
        /// <param name="name"></param>
        /// <param name="requesterConnectAddress"></param>
        public static ZSocket LPClient_CreateZSocket(ZContext context, string name, string requesterConnectAddress, out ZError error)
        {
            var requester = ZSocket.Create(context, ZSocketType.REQ);
            requester.IdentityString = name;
            //requester.Linger = TimeSpan.FromMilliseconds(AppSetting.WAITINGMS);

            if (!requester.Connect(requesterConnectAddress, out error))
            {
                return null;
            }
            return requester;
        }

        public static void LazyPirateClient(string name = "CLIENT", string requesterConnectAddress = "tcp://127.0.0.1:5555")
        {
            using (var context = ZContext.Create())
            {
                ZSocket requester = null;
                try
                {
                    ZError error = null;
                    if ((requester = LPClient_CreateZSocket(context, name, requesterConnectAddress, out error)) == null)
                    {
                        if (error == ZError.ETERM) return;  // Interrupted
                        throw new ZException(error);
                    }

                    int sequence = 0;
                    int retries_left = LPClient_RequestRetries;
                    var poll = ZPollItem.CreateReceiver();

                    while (retries_left > 0)
                    {
                        // We send a request, then we work to get a reply
                        using (var outgoing = ZFrame.Create(4))
                        {
                            outgoing.Write(++sequence);
                            if (!requester.Send(outgoing, out error))
                            {
                                if (error == ZError.ETERM) return;  // Interrupted
                                throw new ZException(error);
                            }
                        }

                        ZMessage incoming = null;
                        while (true)
                        {
                            // Here we process a server reply and exit our loop
                            // if the reply is valid.

                            // If we didn't a reply, we close the client socket
                            // and resend the request. We try a number of times
                            // before finally abandoning:

                            // Poll socket for a reply, with timeout
                            if (requester.PollIn(poll, out incoming, out error, LPClient_RequestTimeout))
                            {
                                using (incoming)
                                {
                                    // We got a reply from the server
                                    int incoming_sequence = incoming[0].ReadInt32();
                                    if (sequence == incoming_sequence)
                                    {
                                        LogService.Info("{0}: server replied OK ({1})", Thread.CurrentThread.Name, incoming_sequence);
                                        retries_left = LPClient_RequestRetries;
                                        break;
                                    }
                                    else
                                    {
                                        LogService.Error("{0}: malformed reply from server", Thread.CurrentThread.Name);
                                    }
                                }
                            }
                            else
                            {
                                if (error == ZError.EAGAIN)
                                {
                                    if (--retries_left == 0)
                                    {
                                        LogService.Error("{0}: server seems to be offline, abandoning", Thread.CurrentThread.Name);
                                        break;
                                    }

                                    LogService.Warn("{0}: no response from server, retrying...", Thread.CurrentThread.Name);

                                    // Old socket is confused; close it and open a new one
                                    requester.Dispose();
                                    if ((requester = LPClient_CreateZSocket(context, name, requesterConnectAddress, out error)) == null)
                                    {
                                        if (error == ZError.ETERM) return;    // Interrupted
                                        throw new ZException(error);
                                    }

                                    LogService.Info("{0}: reconnected");

                                    // Send request again, on new socket
                                    using (var outgoing = ZFrame.Create(4))
                                    {
                                        outgoing.Write(sequence);
                                        if (!requester.Send(outgoing, out error))
                                        {
                                            if (error == ZError.ETERM) return;  // Interrupted
                                            throw new ZException(error);
                                        }
                                    }

                                    continue;
                                }

                                if (error == ZError.ETERM) return;  // Interrupted
                                throw new ZException(error);
                            }
                        }
                    }
                }
                finally
                {
                    if (requester != null)
                    {
                        requester.Dispose();
                        requester = null;
                    }
                }
            }
        }
    }
}
