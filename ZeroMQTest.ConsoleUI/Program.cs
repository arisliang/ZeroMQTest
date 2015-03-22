using Lycn.Common.Aspects;
using Lycn.Common.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ZeroMQ;
using ZeroMQTest.Common;
using ZeroMQTest.Common.Patterns;

namespace ZeroMQTest.ConsoleUI
{
    class Program
    {
        static void Main(string[] args)
        {
            LogService.Name = "ZeroMQ Test";
            LogService.Debug("Application started.");

            try
            {
                //HelloWorldTest();
                //WeatherUpdateTest();
                //ParallelTaskTest();
                //RequestReplyTest();
                //MessageQueueBrokerTest();
                //WeatherUpdateProxyTest();
                //ParallelTaskWithKillTest();
                //HandlingInterruptSignalsTest();
                //MultithreadedServiceTest();
                //MultithreadedRelayTest();
                //NodeCoordinationTest();
                //PubSubEnvelopeTest();
                //RouterReqTest();
                //RouterDealerTest();
                //LBBrokerTest();
                //AsyncSrvTest();
                //Peer1Test();
                Peer2Test();
            }
            catch (ZException ex)
            {
                LogService.Error("{0}\n{1}", ex.Message, ex.StackTrace);
            }
            catch (Exception ex)
            {
                LogService.Error("{0}\n{1}", ex.Message, ex.StackTrace);
            }

            LogService.Debug("Application stopped.");
            Console.ReadKey(true);
        }

        [TraceAspect]
        static void HelloWorldTest()
        {
            using (var context = ZContext.Create())
            {
                var server = new Thread(() => HelloWorld.HWServer(context, "tcp://*:5555"));
                var client = new Thread(() => HelloWorld.HWClient(context, "tcp://127.0.0.1:5555"));

                server.Start();
                client.Start();

                client.Join();
                server.Abort();
            }
        }

        [TraceAspect]
        static void WeatherUpdateTest()
        {
            using (var context = ZContext.Create())
            {
                var server = new Thread(() => WeatherUpdate.WUServer(context, "tcp://*:5556"));
                server.Start();

                int numOfClients = 6;
                var clients = new List<Thread>(numOfClients);
                for (int i = 0; i < numOfClients; ++i)
                {
                    var client = new Thread(() => WeatherUpdate.WUClient(context, "tcp://127.0.0.1:5556"));
                    client.Name = "Client " + i;
                    clients.Add(client);

                    client.Start();
                }

                foreach (var t in clients)
                {
                    t.Join();
                }
                server.Abort();
            }
        }

        [TraceAspect]
        static void ParallelTaskTest()
        {
            using (var context = ZContext.Create())
            {
                var vent = new Thread(() => ParallelTask.TaskVent(context, "tcp://*:5557", "tcp://127.0.0.1:5558"));
                var sink = new Thread(() => ParallelTask.TaskSink(context, "tcp://*:5558"));
                sink.Start();
                vent.Start();

                int numOfWorks = 4;
                var workers = new List<Thread>(numOfWorks);
                for (int i = 0; i < numOfWorks; ++i)
                {
                    var worker = new Thread(() => ParallelTask.TaskWork(context, "tcp://127.0.0.1:5557", "tcp://127.0.0.1:5558"));
                    worker.Name = "Worker " + i;
                    workers.Add(worker);
                    worker.Start();
                }

                sink.Join();
                vent.Join();
                foreach (var t in workers)
                {
                    t.Abort();
                }
            }
        }

        [TraceAspect]
        static void RequestReplyTest()
        {
            var broker = new Thread(() => RequestReply.RRBroker());
            broker.Name = "Broker";
            broker.Start();

            Thread.Sleep(AppSetting.WAITINGMS);

            int numOfClients = 4;
            int numOfWorkers = 2;
            var clients = new List<Thread>(numOfClients);
            var workers = new List<Thread>(numOfWorkers);

            for (int i = 0; i < numOfWorkers; i++)
            {
                var worker = new Thread(() => RequestReply.RRWorker());
                worker.Name = "Worker " + i;
                worker.Start();
            }

            Thread.Sleep(AppSetting.WAITINGMS);

            for (int i = 0; i < numOfClients; i++)
            {
                var client = new Thread(() => RequestReply.RRClient());
                client.Name = "Client " + i;
                client.Start();
            }

            Thread.Sleep(AppSetting.WAITINGMS);

            foreach (var client in clients)
            {
                client.Join();
            }
            foreach (var worker in workers)
            {
                worker.Abort();
            }
            broker.Abort();
        }

        [TraceAspect]
        static void MessageQueueBrokerTest()
        {
            var broker = new Thread(() => MessageQueueBroker.MsgQueue());
            broker.Name = "Msg Broker";
            broker.Start();

            Thread.Sleep(AppSetting.WAITINGMS);

            int numOfClients = 4;
            int numOfWorkers = 2;
            var clients = new List<Thread>(numOfClients);
            var workers = new List<Thread>(numOfWorkers);

            for (int i = 0; i < numOfWorkers; i++)
            {
                var worker = new Thread(() => RequestReply.RRWorker());
                worker.Name = "Worker " + i;
                worker.Start();
            }

            Thread.Sleep(AppSetting.WAITINGMS);

            for (int i = 0; i < numOfClients; i++)
            {
                var client = new Thread(() => RequestReply.RRClient());
                client.Name = "Client " + i;
                client.Start();
            }

            Thread.Sleep(AppSetting.WAITINGMS);

            foreach (var client in clients)
            {
                client.Join();
            }
            foreach (var worker in workers)
            {
                worker.Abort();
            }
            broker.Abort();
        }

        [TraceAspect]
        static void WeatherUpdateProxyTest()
        {
            using (var context = ZContext.Create())
            {
                var server = new Thread(() => WeatherUpdate.WUServer(context));
                server.Start();
                Thread.Sleep(AppSetting.WAITINGMS);

                var proxy = new Thread(() => WeatherUpdateProxy.WUProxy());
                proxy.Start();
                Thread.Sleep(AppSetting.WAITINGMS);

                int numOfClients = 6;
                var clients = new List<Thread>(numOfClients);
                for (int i = 0; i < numOfClients; ++i)
                {
                    var client = new Thread(() => WeatherUpdate.WUClient(context));
                    client.Name = "Client " + i;
                    clients.Add(client);

                    client.Start();
                }

                foreach (var t in clients)
                {
                    t.Join();
                }
                server.Abort();
            }
        }

        [TraceAspect]
        static void ParallelTaskWithKillTest()
        {
            using (var context = ZContext.Create())
            {
                var vent = new Thread(() => ParallelTaskWithKill.TaskVent(context, "tcp://*:5557", "tcp://127.0.0.1:5558"));
                var sink = new Thread(() => ParallelTaskWithKill.TaskSink(context, "tcp://*:5558"));
                sink.Start();
                vent.Start();

                int numOfWorks = 4;
                var workers = new List<Thread>(numOfWorks);
                for (int i = 0; i < numOfWorks; ++i)
                {
                    var worker = new Thread(() => ParallelTaskWithKill.TaskWork(context, "tcp://127.0.0.1:5557", "tcp://127.0.0.1:5558"));
                    worker.Name = "Worker " + i;
                    workers.Add(worker);
                    worker.Start();
                }

                sink.Join();
                vent.Join();
                foreach (var t in workers)
                {
                    t.Abort();
                }
            }
        }

        [TraceAspect]
        static void HandlingInterruptSignalsTest()
        {
            using (var context = ZContext.Create())
            {
                var server = new Thread(() => HandlingInterruptSignals.Interrupt());
                var client = new Thread(() => HelloWorld.HWClient(context, "tcp://127.0.0.1:5555"));

                server.Start();
                client.Start();

                client.Join();
                server.Abort();
            }
        }

        [TraceAspect]
        static void MultithreadedServiceTest()
        {
            using (var context = ZContext.Create())
            {
                var client = new Thread(() => HelloWorld.HWClient(context, "tcp://127.0.0.1:5555"));
                client.Name = "[Client]";
                client.Start();
                MultithreadedService.MTServer();

                client.Join();
            }
        }

        [TraceAspect]
        static void MultithreadedRelayTest()
        {
            var thread = new Thread(() => MultithreadedRelay.MTRelay_step3());
            thread.Name = "Relay Entry: Step 3";
            thread.Start();
        }

        [TraceAspect]
        static void NodeCoordinationTest()
        {
            int numOfSubscribers = 10;

            var publisher = new Thread(() => NodeCoordination.SyncPub(numOfSubscribers));
            publisher.Name = "[Publisher]";
            publisher.Start();

            var subscribers = new List<Thread>(numOfSubscribers);
            for (int i = 0; i < numOfSubscribers; i++)
            {
                var subscriber = new Thread(() => NodeCoordination.SyncSub());
                subscriber.Name = string.Format("[Subscriber {0}]", i);
                subscriber.Start();
            }
        }

        [TraceAspect]
        static void PubSubEnvelopeTest()
        {
            var publisher = new Thread(() =>
            {
                PubSubEnvelope.PSEnvPub();
            });
            publisher.Name = "[Publisher]";
            publisher.Start();

            int numOfSubscribers = 3;
            var subscribers = new List<Thread>();
            for (int i = 0; i < numOfSubscribers; i++)
            {
                var subscriber = new Thread(() =>
                {
                    PubSubEnvelope.PSEnvSub();
                });
                subscriber.Name = string.Format("[Subscriber {0}]", i);
                subscriber.Start();
            }
        }

        [TraceAspect]
        static void RouterReqTest()
        {
            int numOfWorkers = 10;

            var broker = new Thread(() =>
            {
                RouterReq.RTReq_Broker(numOfWorkers);
            });
            broker.Name = "Broker";
            broker.Start();

            Thread.Sleep(AppSetting.WAITINGMS);

            var workers = new List<Thread>(numOfWorkers);
            for (int i = 0; i < numOfWorkers; i++)
            {
                var worker = new Thread(() =>
                {
                    RouterReq.RTReq_Worker(i);
                });
                worker.Name = "Worker " + i;
                worker.Start();

                Thread.Sleep(AppSetting.WAITINGMS);
            }

            broker.Join();

            foreach (var worker in workers)
            {
                worker.Join();
            }
        }

        [TraceAspect]
        static void RouterDealerTest()
        {
            int numOfDealers = 10;

            var broker = new Thread(() =>
            {
                RouterDealer.RTDealer_Broker(numOfDealers);
            });
            broker.Name = "Broker";
            broker.Start();

            Thread.Sleep(AppSetting.WAITINGMS);

            var workers = new List<Thread>(numOfDealers);
            for (int i = 0; i < numOfDealers; i++)
            {
                var worker = new Thread(() =>
                {
                    RouterDealer.RTDealer_Worker(i);
                });
                worker.Name = "Dealer " + i;
                worker.Start();

                Thread.Sleep(AppSetting.WAITINGMS);
            }

            foreach (var worker in workers)
            {
                worker.Join();
                LogService.Debug("{0}: done.", worker.Name);
            }

            broker.Join();
            LogService.Debug("{0}: done.", broker.Name);
        }

        [TraceAspect]
        static void LBBrokerTest()
        {
            int numOfClients = 10;
            int numofWorkers = 3;

            using (var context = ZContext.Create())
            {
                // broker
                var broker = new Thread(() =>
                {
                    LBBroker.LBBroker_Broker(context, numOfClients);
                });
                broker.Name = "Broker";
                broker.Start();

                Thread.Sleep(AppSetting.WAITINGMS);

                // workers
                var workers = new List<Thread>(numofWorkers);
                for (int i = 0; i < numofWorkers; i++)
                {
                    var worker = new Thread(() =>
                    {
                        LBBroker.LBBroker_Worker(context, i);
                    });
                    worker.Name = "Worker " + i;
                    worker.Start();

                    Thread.Sleep(AppSetting.WAITINGMS);
                }

                // clients
                var clients = new List<Thread>(numOfClients);
                for (int i = 0; i < numOfClients; i++)
                {
                    var worker = new Thread(() =>
                    {
                        LBBroker.LBBroker_Client(context, i);
                    });
                    worker.Name = "Client " + i;
                    worker.Start();

                    Thread.Sleep(AppSetting.WAITINGMS);
                }

                foreach (var worker in workers)
                {
                    worker.Join();
                    LogService.Debug("{0}: done.", worker.Name);
                }

                foreach (var client in clients)
                {
                    client.Join();
                    LogService.Debug("{0}: done.", client.Name);
                }

                broker.Join();
                LogService.Debug("{0}: done.", broker.Name);
            }
        }

        [TraceAspect]
        static void AsyncSrvTest()
        {
            // The main thread simply starts several clients and a server, and then
            // waits for the server to finish.
            using (var context = ZContext.Create())
            {
                int numOfClients = 5;
                for (int i = 0; i < numOfClients; i++)
                {
                    var client = new Thread(() => AsyncServer.AsyncSrv_Client(context, i));
                    client.Name = "CLIENT " + i;
                    client.Start();
                    Thread.Sleep(AppSetting.WAITINGMS);
                }

                var server = new Thread(() => AsyncServer.AsyncSrv_ServerTask(context));
                server.Name = "SERVER";
                server.Start();
                Thread.Sleep(AppSetting.WAITINGMS);

                int numOfWorkers = 5;
                for (int i = 0; i < numOfWorkers; i++)
                {
                    var worker = new Thread(() => AsyncServer.AsyncSrv_ServerWorker(context, i));
                    worker.Name = "WORKER " + i;
                    worker.Start();
                    Thread.Sleep(AppSetting.WAITINGMS);
                }

                // Run for 5 seconds then quit
                Thread.Sleep(5 * 1000);
            }
        }

        [TraceAspect]
        private static void Peer1Test()
        {
            int numOfDCs = 3;
            for (int i = 0; i < numOfDCs; i++)
            {
                var peers = new List<string>();
                for (int j = i; j < numOfDCs + i - 1; j++)
                {
                    peers.Add("DC" + (j + 1) % numOfDCs);
                }

                string dcName = "DC" + i;
                var dc = new Thread(() => Peer1.Peering1(dcName, i, peers.ToArray()));
                dc.Name = dcName;
                dc.Start();
                Thread.Sleep(1000);
            }
        }

        private static void Peer2Test()
        {
            int Peering2_Clients = 10;
            int Peering2_Workers = 3;
            int numOfDCs = 3;
            string message = "TASK";

            for (int i = 0; i < numOfDCs; i++)
            {
                var peers = new HashSet<string>();
                for (int j = i; j < numOfDCs + i - 1; j++)
                {
                    peers.Add("DC" + (j + 1) % numOfDCs);
                }

                string dcName = "DC" + i;
                var dc = new Thread(() =>
                {
                    Peer2.Peering2(dcName, message, peers.ToArray(), Peering2_Workers, Peering2_Clients);
                });
                dc.Name = dcName;
                dc.Start();
                Thread.Sleep(1000);
            }
        }
    }
}
