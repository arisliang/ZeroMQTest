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
    /// Broker peering simulation (part 1)
    /// Prototypes the state flow
    /// </summary>
    public static class Peer1
    {
        /// <summary>
        /// First argument is this broker's name
        /// Other arguments are our peers' names
        /// </summary>
        public static void Peering1(string selfName, int selfId, string[] peerNames, string baseAddress = "tcp://127.0.0.1:")
        {
            using (var context = ZContext.Create())
            {
                using (var backend = ZSocket.Create(context, ZSocketType.PUB))
                {
                    string selfAddress = baseAddress + Peering1_GetPort(selfName, selfId);
                    backend.Bind(selfAddress);
                    LogService.Trace("{0}: {1} backend binding on {2}.", Thread.CurrentThread.Name, selfName, selfAddress);

                    using (var frontend = ZSocket.Create(context, ZSocketType.SUB))
                    {
                        // Connect frontend to all peers
                        frontend.SubscribeAll();
                        for (int i = 0; i < peerNames.Length; i++)
                        {
                            string peer = peerNames[i];
                            string peerAddress = baseAddress + Peering1_GetPort(peer, selfId);
                            LogService.Trace("{0}: {1} frontend connecting to state backend at {2}",
                                Thread.CurrentThread.Name, peer, peerAddress);
                            frontend.Connect(peerAddress);
                        }

                        // The main loop sends out status messages to peers, and collects
                        // status messages back from peers. The zmq_poll timeout defines
                        // our own heartbeat:
                        ZError error = null;
                        ZMessage incoming = null;
                        var poll = ZPollItem.CreateReceiver();
                        var rnd = new Random();

                        while (true)
                        {
                            // Poll for activity, or 1 second timeout
                            if (!frontend.PollIn(poll, out incoming, out error, TimeSpan.FromMilliseconds(1000)))
                            {
                                if (error == ZError.EAGAIN)
                                {
                                    error = ZError.None;

                                    using (var output = new ZMessage())
                                    {
                                        output.Add(new ZFrame(selfName));
                                        var outputNumber = ZFrame.Create(4);
                                        outputNumber.Write(rnd.Next(10));
                                        output.Add(outputNumber);

                                        backend.Send(output);
                                    }

                                    continue;
                                }
                                if (error == ZError.ETERM) return;  // Interrupted

                                throw new ZException(error);
                            }
                            using (incoming)
                            {
                                string peer_name = incoming[0].ReadString();
                                int available = incoming[1].ReadInt32();
                                LogService.Debug("{0} - {1} workers free", peer_name, available);
                            }
                        }
                    }
                }
            }
        }

        static Int16 Peering1_GetPort(string name, int i)
        {
            var hash = (Int16)name[0];
            if (hash < 1024)
            {
                hash += (short)(1024 + i);
            }
            return hash;
        }
    }
}
