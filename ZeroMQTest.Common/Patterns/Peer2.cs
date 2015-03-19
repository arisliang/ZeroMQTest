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
        /// <param name="context"></param>
        public static void Peering2(ZContext context, string brokerName, string[] peerNames, int numOfWorkers,
            string cloudFrontendBindBaseAddress = "tcp://127.0.0.1:", string cloudBackendConnectBaseAddress = "tcp://127.0.0.1:",
            string localFrontendBindBaseAddress = "tcp://127.0.0.1:", string localBackendBindBaseAddress = "tcp://127.0.0.1:")
        {
            Contract.Requires(context != null);
            LogService.Trace("{0}: preparing broker as {1}", Thread.CurrentThread.Name, brokerName);

            using (var cloudFrontend = ZSocket.Create(context, ZSocketType.ROUTER))
            {
                using (var cloudBackend = ZSocket.Create(context, ZSocketType.ROUTER))
                {
                    using (var localFrontend = ZSocket.Create(context, ZSocketType.ROUTER))
                    {
                        using (var localBackend = ZSocket.Create(context, ZSocketType.ROUTER))
                        {
                            // Bind cloud frontend to endpoint
                            cloudFrontend.IdentityString = brokerName;
                            {
                                string ep = cloudFrontendBindBaseAddress + Peering2_GetPort(brokerName) + 0;
                                cloudFrontend.Bind(ep);
                                LogService.Trace("{0}: cloud frontend of broker {1} binded on {2}.", Thread.CurrentThread.Name, brokerName, ep);
                            }


                            // Connect cloud backend to all peers
                            cloudBackend.IdentityString = brokerName;
                            for (int i = 0; i < peerNames.Length; i++)
                            {
                                string peer = peerNames[i];
                                {
                                    string ep = cloudBackendConnectBaseAddress + Peering2_GetPort(peer) + 0;
                                    LogService.Trace("{0}: cloud backend of broker {1} connecting to cloud frontend of broker peer {2} at {3}.",
                                        Thread.CurrentThread.Name, brokerName, peer, ep);
                                    cloudBackend.Connect(ep);
                                }
                            }

                            // Prepare local frontend and backend
                            {
                                string ep = localFrontendBindBaseAddress + Peering2_GetPort(brokerName) + 1;
                                localFrontend.Bind(ep);
                                LogService.Trace("{0}: local frontend of broker {1} binded on {2}.", Thread.CurrentThread.Name, brokerName, ep);
                            }
                            {
                                string ep = localBackendBindBaseAddress + Peering2_GetPort(brokerName) + 2;
                                localBackend.Bind(ep);
                                LogService.Trace("{0}: local backend of broker {1} binded on {2}.", Thread.CurrentThread.Name, brokerName, ep);
                            }

                            // Get user to tell us when we can start…
                            LogService.Warn("Press ENTER when all brokers are started...");
                            Console.ReadKey(true);

                            // Start local workers
                            for (int i = 0; i < numOfWorkers; i++)
                            {

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
