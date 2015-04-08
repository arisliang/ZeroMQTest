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
    public static class SimplePiratePattern
    {
        public static void SimplePirate_Broker(int numOfClients, string clientBindAddress = "tcp://*:5555",
            string workerBindAddress = "tcp://*:5556")
        {
            using (var context = ZContext.Create())
            {
                LBBroker.LBBroker_Broker(context, numOfClients, clientBindAddress, workerBindAddress);
            }
        }

        /// <summary>
        /// Simple Pirate worker
        /// Connects REQ socket to tcp://localhost:5556
        /// Implements worker part of load-balancing
        /// </summary>
        public static void SimplePirate_Worker(int i, string workerConnectAddress = "tcp://127.0.0.1:5556")
        {
            using (var context = ZContext.Create())
            {
                using (var worker = ZSocket.Create(context, ZSocketType.REQ))
                {
                    var name = "WORKER" + i;
                    worker.Identity = Encoding.UTF8.GetBytes(name);
                    worker.Connect(workerConnectAddress);

                    LogService.Debug("{0}: {1} worker ready", Thread.CurrentThread.Name, name);

                    using (var outgoing = new ZFrame("READY"))
                    {
                        worker.Send(outgoing);
                    }

                    int cycles = 0;
                    int crashCycle = 16;
                    int overloadCycle = 4;
                    int overloadMs = 1000;

                    ZError error = null;
                    ZMessage incoming = null;

                    var rnd = new Random();

                    while (true)
                    {
                        if ((incoming = worker.ReceiveMessage(out error)) == null)
                        {
                            if (error == ZError.ETERM) return;
                            throw new ZException(error);
                        }
                        using (incoming)
                        {
                            // Simulate various problems, after a few cycles
                            cycles++;

                            if (cycles > crashCycle && rnd.Next(crashCycle) == 0)
                            {
                                LogService.Info("{0}: simulating a crash", Thread.CurrentThread.Name);
                                return;
                            }
                            else if (cycles > overloadCycle && rnd.Next(overloadCycle) == 0)
                            {
                                LogService.Warn("{0}: simulating CPU overload", Thread.CurrentThread.Name);
                                Thread.Sleep(overloadMs);
                            }

                            LogService.Debug("{0}: normal request ({1})", Thread.CurrentThread.Name, incoming[0].ReadInt32());
                            Thread.Sleep(AppSetting.WAITINGMS); // Do some heavy work

                            worker.Send(incoming);
                        }
                    }
                }
            }
        }

        public static void SimplePirate_Client(string name = "CLIENT", string requesterConnectAddress = "tcp://127.0.0.1:5555")
        {
            LazyPiratePattern.LazyPirateClient(name, requesterConnectAddress);
        }
    }
}
