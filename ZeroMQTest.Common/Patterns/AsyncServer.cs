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
    /// Asynchronous client-to-server (DEALER to ROUTER)
    /// </summary>
    public static class AsyncServer
    {
        /// <summary>
        /// This is our client task
        /// It connects to the server, and then sends a request once per second
        /// It collects responses as they arrive, and it prints them out. We will
        /// run several client tasks in parallel, each with a different random ID.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="i"></param>
        public static void AsyncSrv_Client(ZContext context, int i, string clientConnectAddress = "tcp://localhost:5570")
        {
            Contract.Requires(context != null);

            using (var client = ZSocket.Create(context, ZSocketType.DEALER))
            {
                // Set identity to make tracing easier
                client.Identity = Encoding.UTF8.GetBytes("CLIENT" + i);
                // Connect
                ZError error = null;
                if (!client.Connect(clientConnectAddress, out error))
                {
                    LogService.Warn("{0}: Failed to connect to {1} with error {2}.",
                        Thread.CurrentThread.Name, clientConnectAddress, error.ToString());
                    if (error == ZError.ETERM) return;  // Interrupted
                    throw new ZException(error);
                }
                LogService.Trace("{0}: Successfully connected to {1}.", Thread.CurrentThread.Name, clientConnectAddress);

                ZMessage incoming = null;
                var poll = ZPollItem.CreateReceiver();

                int requests = 0;
                int pollMs = 10;
                while (true)
                {
                    // Tick once per second, pulling in arriving messages
                    for (int centitick = 0; centitick < AppSetting.TICKS; ++centitick)
                    {
                        if (!client.PollIn(poll, out incoming, out error, TimeSpan.FromMilliseconds(pollMs)))
                        {
                            if (error == ZError.EAGAIN)
                            {
                                error = ZError.None;
                                continue;
                            }
                            if (error == ZError.ETERM) return;  // Interrupted
                            throw new ZException(error);
                        }
                        using (incoming)
                        {
                            string messageText = incoming[0].ReadString();
                            LogService.Info("{0}: [RECEIVED] {1}.", Thread.CurrentThread.Name, messageText);
                        }
                    }
                    using (var outgoing = new ZMessage())
                    {
                        outgoing.Add(new ZFrame(client.Identity));
                        string requestText = "request " + (++requests);
                        outgoing.Add(new ZFrame(requestText));
                        if (!client.Send(outgoing, out error))
                        {
                            if (error == ZError.ETERM) return;  // Interrupted
                            throw new ZException(error);
                        }
                        LogService.Debug("{0}: [SENDING] {1}: {2}.", Thread.CurrentThread.Name, client.IdentityString, requestText);
                    }
                }
            }
        }

        /// <summary>
        /// This is our server task.
        /// It uses the multithreaded server model to deal requests out to a pool
        /// of workers and route replies back to clients. One worker can handle
        /// one request at a time but one client can talk to multiple workers at
        /// once.
        /// </summary>
        /// <param name="context"></param>
        public static void AsyncSrv_ServerTask(ZContext context, string frontendBindAddress = "tcp://*:5570",
            string backendBindAddress = "inproc://backend")
        {
            Contract.Requires(context != null);

            using (var frontend = ZSocket.Create(context, ZSocketType.ROUTER))
            {
                ZError error = null;

                // Frontend socket talks to clients over TCP
                if (!frontend.Bind(frontendBindAddress, out error))
                {
                    LogService.Warn("{0}: Frontend binding on {1} failed with error {2}", Thread.CurrentThread.Name,
                        frontendBindAddress, error.ToString());
                    if (error == ZError.ETERM) return;  // Interrupted
                    throw new ZException(error);
                }
                LogService.Trace("{0}: Frontend binding on {1} successfully.", Thread.CurrentThread.Name, frontendBindAddress);

                using (var backend = ZSocket.Create(context, ZSocketType.DEALER))
                {
                    // Backend socket talks to workers over inproc
                    if (!backend.Bind(backendBindAddress, out error))
                    {
                        LogService.Warn("{0}: Backend binding on {1} failed with error {2}", Thread.CurrentThread.Name,
                            backendBindAddress, error.ToString());
                        if (error == ZError.ETERM) return;  // Interrupted
                        throw new ZException(error);
                    }
                    LogService.Trace("{0}: Backend binding on {1} successfully.", Thread.CurrentThread.Name, backendBindAddress);

                    // Connect backend to frontend via a proxy
                    if (!ZContext.Proxy(frontend, backend, out error))
                    {
                        if (error == ZError.ETERM) return;  // Interrupted
                        throw new ZException(error);
                    }
                    LogService.Debug("proxy created.");
                }
            }
        }

        /// <summary>
        /// Each worker task works on one request at a time and sends a random number
        /// of replies back, with random delays between replies:
        /// </summary>
        /// <param name="context"></param>
        /// <param name="i"></param>
        public static void AsyncSrv_ServerWorker(ZContext context, int i, string workerConnectAddress = "inproc://backend")
        {
            Contract.Requires(context != null);

            using (var worker = ZSocket.Create(context, ZSocketType.DEALER))
            {
                ZError error = null;
                if (!worker.Connect(workerConnectAddress, out error))
                {
                    LogService.Warn("{0}: worker connecting to {1} failed with error {2}", Thread.CurrentThread.Name,
                            workerConnectAddress, error.ToString());
                    if (error == ZError.ETERM) return;  // Interrupted
                    throw new ZException(error);
                }
                LogService.Trace("{0}: worker connecting to {1} successfully.", Thread.CurrentThread.Name, workerConnectAddress);

                ZMessage request = null;
                var rnd = new Random();
                int numReplies = 5;
                while (true)
                {
                    if (null == (request = worker.ReceiveMessage(out error)))
                    {
                        if (error == ZError.ETERM) return;    // Interrupted
                        throw new ZException(error);
                    }


                    using (request)
                    {
                        // The DEALER socket gives us the reply envelope and message
                        string identity = request[0].ReadString();
                        string content = request[2].ReadString();

                        LogService.Debug("{0}: [RECEIVED] {1}: {2}.", Thread.CurrentThread.Name, identity, content);

                        // Send 0..4 replies back
                        int replies = rnd.Next(numReplies);
                        for (int reply = 0; reply < replies; ++reply)
                        {
                            // Sleep for some fraction of a second
                            Thread.Sleep(rnd.Next(1000) + 1);

                            using (var response = new ZMessage())
                            {
                                response.Add(new ZFrame(identity));
                                response.Add(new ZFrame(content));

                                if (!worker.Send(response, out error))
                                {
                                    if (error == ZError.ETERM) return;    // Interrupted
                                    throw new ZException(error);
                                }
                                LogService.Debug("{0}: [SEND] {1}: {2}.", Thread.CurrentThread.Name, identity, content);
                            }
                        }
                    }
                }
            }
        }
    }
}
