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
    public static class ParanoidPiratePattern
    {
        public static void PP_Broker(string clientBindAddress = "tcp://*:5555",
            string workerBindAddress = "tcp://*:5556")
        {
            using (var context = ZContext.Create())
            {
                using (var backend = ZSocket.Create(context, ZSocketType.ROUTER))
                {
                    backend.Bind(workerBindAddress);
                    using (var frontend = ZSocket.Create(context, ZSocketType.ROUTER))
                    {
                        frontend.Bind(clientBindAddress);

                        // List of available workers
                        var workers = new List<PPP_Worker>();

                        // Send out heartbeats at regular intervals
                        DateTime heartbeat_at = DateTime.UtcNow + PPP_Worker.PPP_HEARTBEAT_INTERVAL;

                        // Create a Receiver ZPollItem (ZMQ_POLLIN)
                        var poll = ZPollItem.CreateReceiver();

                        ZError error = null;
                        ZMessage incoming = null;

                        while (true)
                        {
                            // Handle worker activity on backend
                            if (backend.PollIn(poll, out incoming, out error, PPP_Worker.PPP_TICK))
                            {
                                using (incoming)
                                {
                                    // Any sign of life from worker means it's ready
                                    ZFrame identity = incoming.Unwrap();
                                    var worker = new PPP_Worker(identity);
                                    workers.Ready(worker);

                                    // Validate control message, or return reply to client
                                    if (incoming.Count == 1)
                                    {
                                        string message = incoming[0].ReadString();
                                        if (message == PPP_Worker.PPP_READY)
                                        {
                                            LogService.Debug("{0}: Worker ready ({1})",
                                                Thread.CurrentThread.Name, worker.IdentityString);
                                        }
                                        else if (message == PPP_Worker.PPP_HEARTBEAT)
                                        {
                                            LogService.Debug("{0}: receiving heartbeat ({1})",
                                                Thread.CurrentThread.Name, worker.IdentityString);
                                        }
                                        else
                                        {
                                            LogService.Warn("{0}: E: invalid message {1} from worker {2}",
                                                Thread.CurrentThread.Name, worker.IdentityString);
                                        }
                                    }
                                    else
                                    {
                                        LogService.Warn("{0}: [backend sending to frontend] ({0})",
                                            Thread.CurrentThread.Name, worker.IdentityString);
                                        frontend.Send(incoming);
                                    }
                                }
                            }
                            else
                            {
                                if (error == ZError.ETERM) break;   // Interrupted
                                if (error != ZError.EAGAIN) throw new ZException(error);
                            }

                            // Handle client activity on frontend
                            if (workers.Count > 0)
                            {
                                // Poll frontend only if we have available workers
                                if (frontend.PollIn(poll, out incoming, out error, PPP_Worker.PPP_TICK))
                                {
                                    // Now get next client request, route to next worker
                                    using (incoming)
                                    {
                                        ZFrame workerIdentity = workers.Next();
                                        incoming.Prepend(workerIdentity);

                                        LogService.Debug("{0}: [frontend sending to backend] ({1})",
                                            Thread.CurrentThread.Name, workerIdentity.ReadString());
                                        backend.Send(incoming);
                                    }
                                }
                                else
                                {
                                    if (error == ZError.ETERM) break;    // Interrupted
                                    if (error != ZError.EAGAIN) throw new ZException(error);
                                }
                            }

                            // We handle heartbeating after any socket activity. First, we send
                            // heartbeats to any idle workers if it's time. Then, we purge any
                            // dead workers:
                            if (DateTime.UtcNow > heartbeat_at)
                            {
                                heartbeat_at = DateTime.UtcNow + PPP_Worker.PPP_HEARTBEAT_INTERVAL;

                                foreach (var worker in workers)
                                {
                                    using (var outgoing = new ZMessage())
                                    {
                                        outgoing.Add(ZFrame.CopyFrom(worker.Identity));
                                        outgoing.Add(new ZFrame(PPP_Worker.PPP_HEARTBEAT));
                                        LogService.Info("{0}: sending heartbeat ({1})",
                                            Thread.CurrentThread.Name, worker.IdentityString);
                                        backend.Send(outgoing);
                                    }
                                }
                            }
                            workers.Purge();
                        }

                        // When we're done, clean up properly
                        foreach (var worker in workers)
                        {
                            worker.Dispose();
                        }
                    }
                }
            }
        }

        public static void PP_Client(string name = "CLIENT", string requesterConnectAddress = "tcp://127.0.0.1:5555")
        {
            LazyPiratePattern.LazyPirateClient(name, requesterConnectAddress);
        }

        public static void PP_Worker(string name = "WORKER")
        {
            ZError error = null;
            using (var context = ZContext.Create())
            {
                ZSocket worker = null;
                try
                {
                    if ((worker = PP_Worker_CreateZSocket(context, name, out error)) == null)
                    {
                        if (error == ZError.ETERM) return;  // Interrupted
                        throw new ZException(error);
                    }

                    // If liveness hits zero, queue is considered disconnected
                    int liveness = PPP_Worker.PPP_HEARTBEAT_LIVENESS;
                    int interval = PPP_Worker.PPP_INTERVAL_INIT;

                    // Send out heartbeats at regular intervals
                    DateTime heartbeat_at = DateTime.UtcNow + PPP_Worker.PPP_HEARTBEAT_INTERVAL;

                    ZMessage incoming = null;
                    int cycle = 0;
                    var poll = ZPollItem.CreateReceiver();
                    var rnd = new Random();

                    while (true)
                    {
                        if (worker.PollIn(poll, out incoming, out error, PPP_Worker.PPP_TICK))
                        {
                            // Get message
                            // - 3-part envelope + content -> request
                            // - 1-part HEARTBEAT -> heartbeat
                            using (incoming)
                            {
                                // To test the robustness of the queue implementation we
                                // simulate various typical problems, such as the worker
                                // crashing or running very slowly. We do this after a few
                                // cycles so that the architecture can get up and running
                                // first:

                                if (incoming.Count >= 3)
                                {
                                    LogService.Info("{0}: receiving reply", Thread.CurrentThread.Name);
                                    cycle++;
                                    if (cycle > 3)
                                    {
                                        if (rnd.Next(5) == 0)
                                        {
                                            LogService.Warn("{0}: simulating a crash");
                                            return;
                                        }
                                        if (rnd.Next(3) == 0)
                                        {
                                            LogService.Warn("{0}: simulating CPU overload");
                                            return;
                                        }
                                    }

                                    Thread.Sleep(1000);
                                    LogService.Info("{0}: sending reply");
                                    worker.Send(incoming);

                                    liveness = PPP_Worker.PPP_HEARTBEAT_LIVENESS;
                                }
                                // When we get a heartbeat message from the queue, it means the
                                // queue was (recently) alive, so we must reset our liveness
                                // indicator:
                                else if (incoming.Count == 1)
                                {
                                    string identity = incoming[0].ReadString();
                                    if (identity == PPP_Worker.PPP_HEARTBEAT)
                                    {
                                        LogService.Debug("{0}: receiving heartbeat");
                                        liveness = PPP_Worker.PPP_HEARTBEAT_LIVENESS;
                                    }
                                    else
                                    {
                                        LogService.Warn("{0}: E: invalid message", Thread.CurrentThread.Name);
                                    }
                                }
                                else
                                {
                                    LogService.Warn("{0}: E: invalid message", Thread.CurrentThread.Name);
                                }
                            }
                            interval = PPP_Worker.PPP_INTERVAL_INIT;
                        }
                        else
                        {
                            if (error == ZError.ETERM) break;    // Interrupted
                            if (error != ZError.EAGAIN) throw new ZException(error);
                        }

                        if (error == ZError.EAGAIN)
                        {
                            // If the queue hasn't sent us heartbeats in a while, destroy the
                            // socket and reconnect. This is the simplest most brutal way of
                            // discarding any messages we might have sent in the meantime:
                            if (--liveness == 0)
                            {
                                LogService.Warn("{0}: W: heartbeat failure, can't reach queue", Thread.CurrentThread.Name);
                                LogService.Warn("{0}: W: reconnecting in {1} ms", Thread.CurrentThread.Name, interval);
                                Thread.Sleep(interval);
                                if (interval < PPP_Worker.PPP_INTERVAL_MAX)
                                {
                                    interval *= 2;
                                }
                                else
                                {
                                    LogService.Error("{0}: E: interrupted");
                                    break;
                                }

                                worker.Dispose();
                                if (null == (worker = PP_Worker_CreateZSocket(context, name, out error)))
                                {
                                    if (error == ZError.ETERM) break;    // Interrupted
                                    throw new ZException(error);
                                }
                                liveness = PPP_Worker.PPP_HEARTBEAT_LIVENESS;
                            }
                        }

                        // Send heartbeat to queue if it's time
                        if (DateTime.UtcNow > heartbeat_at)
                        {
                            heartbeat_at = DateTime.UtcNow + PPP_Worker.PPP_HEARTBEAT_INTERVAL;
                            LogService.Debug("{0}: sending heartbeat", Thread.CurrentThread.Name);
                            using (var outgoing = new ZFrame(PPP_Worker.PPP_HEARTBEAT))
                            {
                                worker.Send(outgoing);
                            }
                        }
                    }
                }
                finally
                {
                    if (worker != null)
                    {
                        worker.Dispose();
                        worker = null;
                    }
                }
            }
        }

        static ZSocket PP_Worker_CreateZSocket(ZContext context, string name, out ZError error,
            string workerOutboundAddr = "tcp://127.0.0.1:5556")
        {
            // Helper function that returns a new configured socket
            // connected to the Paranoid Pirate queue

            var worker = new ZSocket(context, ZSocketType.DEALER);
            worker.IdentityString = name;

            if (!worker.Connect(workerOutboundAddr, out error))
            {
                return null;    // Interrupted
            }

            // Tell queue we're ready for work
            LogService.Debug("{0}: worker ready", Thread.CurrentThread.Name);
            return worker;
        }
    }

    public class PPP_Worker : IDisposable
    {
        public const int PPP_HEARTBEAT_LIVENESS = 3;    // 3-5 is reasonable
        public static readonly TimeSpan PPP_HEARTBEAT_INTERVAL = TimeSpan.FromMilliseconds(500);
        public static readonly TimeSpan PPP_TICK = TimeSpan.FromMilliseconds(250);

        public const string PPP_READY = "READY";
        public const string PPP_HEARTBEAT = "HEARTBEAT";

        public const int PPP_INTERVAL_INIT = 1000;
        public const int PPP_INTERVAL_MAX = 32000;

        public ZFrame Identity;

        public DateTime Expiry;

        public string IdentityString
        {
            get
            {
                Identity.Position = 0;
                return Identity.ReadString();
            }
            set
            {
                if (Identity != null)
                {
                    Identity.Dispose();
                }
                Identity = new ZFrame(value);
            }
        }

        // Construct new worker
        public PPP_Worker(ZFrame identity)
        {
            Identity = identity;

            this.Expiry = DateTime.UtcNow + TimeSpan.FromMilliseconds(
                PPP_HEARTBEAT_INTERVAL.Milliseconds * PPP_HEARTBEAT_LIVENESS);
        }

        // Destroy specified worker object, including identity frame.
        public void Dispose()
        {
            GC.SuppressFinalize(this);
            Dispose(true);
        }

        protected void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (Identity != null)
                {
                    Identity.Dispose();
                    Identity = null;
                }
            }
        }
    }

    public static class PPP_WorkersExtensions
    {
        public static void Ready(this IList<PPP_Worker> workers, PPP_Worker worker)
        {
            workers.Add(worker);
        }

        public static ZFrame Next(this IList<PPP_Worker> workers)
        {
            var worker = workers[0];
            workers.RemoveAt(0);

            ZFrame identity = worker.Identity;
            worker.Identity = null;
            worker.Dispose();

            return identity;
        }

        public static void Purge(this IList<PPP_Worker> workers)
        {
            foreach (PPP_Worker worker in workers.ToList())
            {
                if (DateTime.UtcNow < worker.Expiry)
                {
                    continue;   // Worker is alive, we're done here
                }

                workers.Remove(worker);
            }
        }
    }
}
