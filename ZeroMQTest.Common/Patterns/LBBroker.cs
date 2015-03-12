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
    /// Load-balancing broker in C#
    /// </summary>
    public static class LBBroker
    {
        /// <summary>
        /// Basic request-reply client using REQ socket
        /// </summary>
        public static void LBBroker_Client(ZContext context, int i, string clientConnectAddress = "inproc://frontend")
        {
            Contract.Requires(context != null);

            ZError error = null;

            // Create a socket
            using (var client = ZSocket.Create(context, ZSocketType.REQ))
            {
                // Set a printable identity
                client.IdentityString = "CLIENT" + i;
                // Connect
                client.Connect(clientConnectAddress, out error);
                if (error == null)
                {
                    LogService.Trace("{0}: client connecting to {1}.", Thread.CurrentThread.Name, clientConnectAddress);
                }
                else
                {
                    LogService.Warn("{0}: client connecting to {1} with error {2}.", Thread.CurrentThread.Name, clientConnectAddress, error.ToString());
                }

                using (var request = new ZMessage())
                {
                    string requestText = "Hello";
                    request.Add(new ZFrame(requestText));
                    // Send request
                    LogService.Debug("{0}: Sending request: {1}", Thread.CurrentThread.Name, requestText);
                    client.Send(request);
                }

                // Receive reply
                using (var reply = client.ReceiveMessage())
                {
                    LogService.Info("CLIENT{0}: {1}", i, reply[0].ReadString());
                }
            }
        }

        /// <summary>
        /// This is the worker task, using a REQ socket to do load-balancing.
        /// </summary>
        public static void LBBroker_Worker(ZContext context, int i, string workerConnectAddress = "inproc://backend")
        {
            Contract.Requires(context != null);

            ZError error = null;

            // Create socket
            using (var worker = ZSocket.Create(context, ZSocketType.REQ))
            {
                // Set a printable identity
                worker.IdentityString = "WORKER" + i;

                // Connect
                worker.Connect(workerConnectAddress, out error);
                if (error == null)
                {
                    LogService.Trace("{0}: worker connecting to {1}.", Thread.CurrentThread.Name, workerConnectAddress);
                }
                else
                {
                    LogService.Warn("{0}: worker connecting to {1} with error {2}.", Thread.CurrentThread.Name, workerConnectAddress, error.ToString());
                }

                // Tell broker we're ready for work
                string readyText = "READY";
                using (var ready = new ZFrame(readyText))
                {
                    LogService.Debug("{0}: Sending message: {1}", Thread.CurrentThread.Name, readyText);
                    worker.Send(ready);
                }

                ZMessage request = null;
                while (true)
                {
                    // Get request
                    if (null == (request = worker.ReceiveMessage(out error)))
                    {
                        // We are using "out error",
                        // to NOT throw a ZException ETERM
                        if (error == ZError.ETERM) break;
                        throw new ZException(error);
                    }

                    using (request)
                    {
                        string worker_id = request[0].ReadString();
                        string requestText = request[2].ReadString();
                        LogService.Debug("{0}: WORKER{0}: {1}", Thread.CurrentThread.Name, i, requestText);

                        // Send reply
                        using (var commit = new ZMessage())
                        {
                            commit.Add(new ZFrame(worker_id));
                            commit.Add(new ZFrame());
                            commit.Add(new ZFrame("OK"));

                            worker.Send(commit);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// This is the main task. It starts the clients and workers, and then
        /// routes requests between the two layers. Workers signal READY when
        /// they start; after that we treat them as ready when they reply with
        /// a response back to a client. The load-balancing data structure is
        /// just a queue of next available workers.
        /// </summary>
        public static void LBBroker_Broker(ZContext context, int numOfClients, string clientBindAddress = "inproc://frontend",
            string workerBindAddress = "inproc://backend")
        {
            Contract.Requires(context != null);

            ZError error = null;

            using (var frontend = ZSocket.Create(context, ZSocketType.ROUTER))
            {
                // Bind
                frontend.Bind(clientBindAddress, out error);
                if (error == null)
                {
                    LogService.Trace("{0}: frontend binding on {1}", Thread.CurrentThread.Name, clientBindAddress);
                }
                else
                {
                    LogService.Warn("{0}: frontend binding on {1} failed with error {2}.", Thread.CurrentThread.Name, clientBindAddress, error.ToString());
                }


                using (var backend = ZSocket.Create(context, ZSocketType.ROUTER))
                {
                    // Bind
                    backend.Bind(workerBindAddress, out error);
                    if (error == null)
                    {
                        LogService.Trace("{0}: backend binding on {1}", Thread.CurrentThread.Name, workerBindAddress);
                    }
                    else
                    {
                        LogService.Warn("{0}: backend binding on {1} failed with error {2}", Thread.CurrentThread.Name, workerBindAddress, error.ToString());
                    }

                    // Here is the main loop for the least-recently-used queue. It has two
                    // sockets; a frontend for clients and a backend for workers. It polls
                    // the backend in all cases, and polls the frontend only when there are
                    // one or more workers ready. This is a neat way to use 0MQ's own queues
                    // to hold messages we're not ready to process yet. When we get a client
                    // reply, we pop the next available worker and send the request to it,
                    // including the originating client identity. When a worker replies, we
                    // requeue that worker and forward the reply to the original client
                    // using the reply envelope.

                    // Queue of available workers
                    var worker_queue = new HashSet<string>();

                    ZMessage incoming = null;

                    var poll = ZPollItem.CreateReceiver();

                    while (true)
                    {
                        //LogService.Trace("{0}: Waiting for incoming message.", Thread.CurrentThread.Name);
                        if (backend.PollIn(poll, out incoming, out error, TimeSpan.FromMilliseconds(AppSetting.POLLMS)))
                        {
                            LogService.Trace("{0}: Reiceved backend message.", Thread.CurrentThread.Name);

                            // Handle worker activity on backend
                            // incoming[0] is worker_id
                            string worker_id = incoming[0].ReadString();
                            // Queue worker identity for load-balancing
                            LogService.Debug("Worker {0} is added to the queue.", worker_id);
                            worker_queue.Add(worker_id);

                            // incoming[1] is empty

                            // incoming[2] is READY or else client_id
                            string client_id = incoming[2].ReadString();
                            if (client_id != "READY")
                            {
                                // incoming[3] is empty

                                // incoming[4] is reply
                                string reply = incoming[4].ReadString();

                                using (var outgoing = new ZMessage())
                                {
                                    outgoing.Add(new ZFrame(client_id));
                                    outgoing.Add(new ZFrame());
                                    outgoing.Add(new ZFrame(reply));

                                    // Send
                                    frontend.Send(outgoing);
                                }

                                if (--numOfClients == 0)
                                {
                                    // break the while (true) when all clients said Hello
                                    break;
                                }
                            }
                        }
                        if (worker_queue.Count > 0)
                        {
                            // Poll frontend only if we have available workers
                            if (frontend.PollIn(poll, out incoming, out error, TimeSpan.FromMilliseconds(AppSetting.POLLMS)))
                            {
                                LogService.Trace("{0}: Reiceved frontend message.", Thread.CurrentThread.Name);

                                // Here is how we handle a client request

                                // incoming[0] is client_id
                                string client_id = incoming[0].ReadString();

                                // incoming[1] is empty

                                // incoming[2] is request
                                string requestText = incoming[2].ReadString();

                                var worker_id = worker_queue.First();
                                using (var outgoing = new ZMessage())
                                {
                                    outgoing.Add(new ZFrame(worker_id));
                                    outgoing.Add(new ZFrame());
                                    outgoing.Add(new ZFrame(client_id));
                                    outgoing.Add(new ZFrame());
                                    outgoing.Add(new ZFrame(requestText));

                                    // Send
                                    backend.Send(outgoing);
                                }

                                // Dequeue the next worker identity
                                LogService.Debug("Worker {0} is removed from the queue.", worker_id);
                                worker_queue.Remove(worker_id);
                            }
                        }

                        //LogService.Trace("{0}: no message from frontend/backend.", Thread.CurrentThread.Name);
                    }
                }
            }
        }
    }
}
