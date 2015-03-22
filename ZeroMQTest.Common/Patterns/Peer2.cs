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
    ///
    /// Broker peering simulation (part 2)
    /// Prototypes the request-reply flow
    //
    /// </summary>
    public static class Peer2
    {
        /// <summary>
        /// The client task does a request-reply dialog
        /// using a standard synchronous REQ socket
        /// </summary>
        public static void Peering2_ClientTask(ZContext context, int i, string name, string message,
            string clientConnectBaseEndPoint = "tcp://127.0.0.1:")
        {
            Contract.Requires(context != null);

            using (var client = ZSocket.Create(context, ZSocketType.REQ))
            {
                // Set printable identity
                client.IdentityString = name;

                // Connect
                string ep = clientConnectBaseEndPoint + Peering2_GetPort(name) + 1;
                client.Connect(ep);
                LogService.Trace("{0}: client connected to {1}.", Thread.CurrentThread.Name, ep);

                ZError error = null;
                while (true)
                {
                    // Send
                    using (var outgoing = new ZFrame(message))
                    {
                        client.Send(outgoing);
                        LogService.Debug("{0}: client {1} sent {2}", Thread.CurrentThread.Name, name, message);
                    }

                    // Receive
                    ZFrame incoming = client.ReceiveFrame(out error);
                    if (incoming == null)
                    {
                        if (error == ZError.ETERM) return;  // Interrupted
                        throw new ZException(error);
                    }
                    using (incoming)
                    {
                        LogService.Debug("{0}: client {1} received {2}", Thread.CurrentThread.Name, name, message);
                    }
                }
            }
        }

        /// <summary>
        /// The worker task plugs into the load-balancer using a REQ socket
        /// </summary>
        /// <param name="context"></param>
        /// <param name="i"></param>
        /// <param name="name"></param>
        public static void Peering2_WorkerTask(ZContext context, int i, string name,
            string workerConnectBaseAddress = "tcp://127.0.0.1:")
        {
            Contract.Requires(context != null);

            using (var worker = ZSocket.Create(context, ZSocketType.REQ))
            {
                // Set printable identity
                worker.IdentityString = name;

                // Connect
                string ep = workerConnectBaseAddress + Peering2_GetPort(name) + 2;
                worker.Connect(ep);

                // Tell broker we're ready for work
                worker.Send(new ZFrame("READY"));

                // Process messages as they arrive
                ZError error = null;
                int workMs = 1000;
                while (true)
                {
                    // Receive
                    ZFrame incoming = worker.ReceiveFrame(out error);

                    if (incoming == null)
                    {
                        if (error == ZError.ETERM) return;  // Interrupted
                        throw new ZException(error);
                    }
                    using (incoming)
                    {
                        LogService.Debug("{0}: worker {1} received {2}.", Thread.CurrentThread.Name, name, incoming.ReadString());
                    }

                    // Do some heavy work
                    Thread.Sleep(workMs);

                    // Send
                    using (var outgoing = new ZFrame("TASK DONE"))
                    {
                        worker.Send(outgoing);
                        LogService.Debug("{0}: worker {1} sent {2}.", Thread.CurrentThread.Name, name, outgoing.ReadString());
                    }
                }
            }
        }

        /// <summary>
        /// The main task begins by setting-up its frontend and backend sockets
        /// and then starting its client and worker tasks:
        /// </summary>
        /// <param name="selfName"></param>
        /// <param name="message">Client's task request message</param>
        /// <param name="peerNames"></param>
        /// <param name="numOfWorkers"></param>
        /// <param name="numOfClients"></param>
        /// <param name="cloudFrontendBindBaseAddress"></param>
        /// <param name="cloudBackendConnectBaseAddress"></param>
        /// <param name="localFrontendBindBaseAddress"></param>
        /// <param name="localBackendBindBaseAddress"></param>
        public static void Peering2(string selfName, string message, string[] peerNames, int numOfWorkers, int numOfClients,
            string cloudFrontendBindBaseAddress = "tcp://127.0.0.1:", string cloudBackendConnectBaseAddress = "tcp://127.0.0.1:",
            string localFrontendBindBaseAddress = "tcp://127.0.0.1:", string localBackendBindBaseAddress = "tcp://127.0.0.1:")
        {
            LogService.Trace("{0}: preparing broker as {1}", Thread.CurrentThread.Name, selfName);

            using (var context = ZContext.Create())
            {
                using (var cloudFrontend = ZSocket.Create(context, ZSocketType.ROUTER))
                {
                    using (var cloudBackend = ZSocket.Create(context, ZSocketType.ROUTER))
                    {
                        using (var localFrontend = ZSocket.Create(context, ZSocketType.ROUTER))
                        {
                            using (var localBackend = ZSocket.Create(context, ZSocketType.ROUTER))
                            {
                                // Bind cloud frontend to endpoint
                                cloudFrontend.IdentityString = selfName;
                                {
                                    string ep = cloudFrontendBindBaseAddress + Peering2_GetPort(selfName) + 0;
                                    cloudFrontend.Bind(ep);
                                    LogService.Trace("{0}: cloud frontend of broker {1} binded on {2}.", Thread.CurrentThread.Name, selfName, ep);
                                }


                                // Connect cloud backend to all peers
                                cloudBackend.IdentityString = selfName;
                                for (int i = 0; i < peerNames.Length; i++)
                                {
                                    string peer = peerNames[i];
                                    {
                                        string ep = cloudBackendConnectBaseAddress + Peering2_GetPort(peer) + 0;
                                        LogService.Trace("{0}: cloud backend of broker {1} connecting to cloud frontend of broker peer {2} at {3}.",
                                            Thread.CurrentThread.Name, selfName, peer, ep);
                                        cloudBackend.Connect(ep);
                                    }
                                }

                                // Prepare local frontend and backend
                                {
                                    string ep = localFrontendBindBaseAddress + Peering2_GetPort(selfName) + 1;
                                    localFrontend.Bind(ep);
                                    LogService.Trace("{0}: local frontend of broker {1} binded on {2}.", Thread.CurrentThread.Name, selfName, ep);
                                }
                                {
                                    string ep = localBackendBindBaseAddress + Peering2_GetPort(selfName) + 2;
                                    localBackend.Bind(ep);
                                    LogService.Trace("{0}: local backend of broker {1} binded on {2}.", Thread.CurrentThread.Name, selfName, ep);
                                }

                                // Get user to tell us when we can start…
                                LogService.Warn("Press ENTER when all brokers are started...");
                                Console.ReadKey(true);

                                // Start local workers
                                for (int i = 0; i < numOfWorkers; i++)
                                {
                                    Thread worker = new Thread(() =>
                                    {
                                        Peering2_WorkerTask(context, i, selfName);
                                    });
                                    worker.Start();
                                }

                                // Start local clients
                                for (int i = 0; i < numOfClients; i++)
                                {
                                    Thread client = new Thread(() =>
                                    {
                                        Peering2_ClientTask(context, i, selfName, message);
                                    });
                                    client.Start();
                                }

                                // Here, we handle the request-reply flow. We're using load-balancing
                                // to poll workers at all times, and clients only when there are one
                                // or more workers available.

                                // Least recently used queue of available workers
                                var workers = new Queue<string>();

                                ZError error = null;
                                ZMessage incoming = null;
                                TimeSpan? wait;
                                var poll = ZPollItem.CreateReceiver();
                                int GotWorkerWaitMs = 1000;

                                while (true)
                                {
                                    // If we have no workers, wait indefinitely
                                    wait = workers.Count > 0 ? (TimeSpan?)TimeSpan.FromMilliseconds(GotWorkerWaitMs) : null;

                                    // Poll localBackend
                                    if (localBackend.PollIn(poll, out incoming, out error, wait))
                                    {
                                        // Handle reply from local worker
                                        string identity = incoming[0].ReadString();
                                        workers.Enqueue(identity);

                                        // If it's READY, don't route the message any further
                                        string hello = incoming[2].ReadString();
                                        if (hello == "READY")
                                        {
                                            incoming.Dispose();
                                            incoming = null;
                                        }
                                    }
                                    else if (error == ZError.EAGAIN && cloudBackend.PollIn(poll, out incoming, out error, wait))
                                    {
                                        // We don't use peer broker identity for anything

                                        // string identity = incoming[0].ReadString();

                                        // string ok = incoming[2].ReadString();
                                    }
                                    else
                                    {
                                        if (error == ZError.ETERM) return;    // Interrupted

                                        if (error != ZError.EAGAIN) throw new ZException(error);
                                    }

                                    if (incoming != null)
                                    {
                                        string identity = incoming[0].ReadString();

                                        var isForCloud = (from cloud in peerNames
                                                          where cloud.Equals(identity, StringComparison.OrdinalIgnoreCase)
                                                          select cloud).Any();

                                        using (incoming)
                                        {
                                            if (isForCloud)
                                            {
                                                // Route reply to cloud if it's addressed to a broker
                                                cloudFrontend.Send(incoming);
                                            }
                                            else
                                            {
                                                // Route reply to client if we still need to
                                                localFrontend.Send(incoming);
                                            }
                                        }

                                        incoming = null;
                                    }

                                    // Now we route as many client requests as we have worker capacity
                                    // for. We may reroute requests from our local frontend, but not from //
                                    // the cloud frontend. We reroute randomly now, just to test things
                                    // out. In the next version, we'll do this properly by calculating
                                    // cloud capacity://
                                    var rnd = new Random();
                                    bool reroutable = false;

                                    while (workers.Count > 0)
                                    {
                                        // We'll do peer brokers first, to prevent starvation
                                        if (localFrontend.PollIn(poll, out incoming, out error, TimeSpan.FromMilliseconds(AppSetting.POLLMS)))
                                        {
                                            reroutable = false;
                                        }
                                        else if (error == ZError.EAGAIN
                                            && cloudFrontend.PollIn(poll, out incoming, out error, TimeSpan.FromMilliseconds(AppSetting.POLLMS)))
                                        {
                                            reroutable = true;
                                        }
                                        else
                                        {
                                            if (error == ZError.ETERM) return;    // Interrupted

                                            if (error == ZError.EAGAIN) break;    // No work, go back to backends

                                            throw new ZException(error);
                                        }

                                        using (incoming)
                                        {
                                            // If reroutable, send to cloud 25% of the time
                                            // Here we'd normally use cloud status information

                                            if (reroutable == true && rnd.Next(4) == 0)
                                            {
                                                // Route to random broker peer
                                                int peer = rnd.Next(peerNames.Length - 2) + 2;
                                                incoming.ReplaceAt(0, new ZFrame(peerNames[peer]));

                                                /*using (var outgoing = new ZMessage())
                                                {
                                                    outgoing.Add(new ZFrame(args[peer]));
                                                    outgoing.Add(new ZFrame());
                                                    outgoing.Add(incoming[2]);

                                                    cloudBackend.Send(outgoing);
                                                }*/

                                                cloudBackend.Send(incoming);
                                            }
                                            else
                                            {
                                                // Route to local broker peer
                                                string peer = workers.Dequeue();
                                                incoming.ReplaceAt(0, new ZFrame(peer));

                                                /*using (var outgoing = new ZMessage())
                                                {
                                                    outgoing.Add(new ZFrame(peer));
                                                    outgoing.Add(new ZFrame());
                                                    outgoing.Add(incoming[2]);

                                                    localBackend.Send(outgoing);
                                                }*/

                                                localBackend.Send(incoming);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

        }

        static Int16 Peering2_GetPort(string name)
        {
            var hash = (Int16)name[0];
            if (hash < 1024)
            {
                hash += 1024;
            }
            return hash;
        }
    }
}
