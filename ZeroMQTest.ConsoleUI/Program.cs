using Lycn.Common.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ZeroMQ;
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
                RouterDealerTest();
            }
            catch (ZException ex)
            {
                LogService.Error(string.Format("{0}\n{1}", ex.Message, ex.StackTrace));
            }

            LogService.Debug("Application stopped.");
            Console.ReadKey(true);
        }

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

        static void RequestReplyTest()
        {
            var broker = new Thread(() => RequestReply.RRBroker());
            broker.Name = "Broker";
            broker.Start();

            Thread.Sleep(1000);

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

            Thread.Sleep(1000);

            for (int i = 0; i < numOfClients; i++)
            {
                var client = new Thread(() => RequestReply.RRClient());
                client.Name = "Client " + i;
                client.Start();
            }

            Thread.Sleep(1000);

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

        static void MessageQueueBrokerTest()
        {
            var broker = new Thread(() => MessageQueueBroker.MsgQueue());
            broker.Name = "Msg Broker";
            broker.Start();

            Thread.Sleep(1000);

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

            Thread.Sleep(1000);

            for (int i = 0; i < numOfClients; i++)
            {
                var client = new Thread(() => RequestReply.RRClient());
                client.Name = "Client " + i;
                client.Start();
            }

            Thread.Sleep(1000);

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

        static void WeatherUpdateProxyTest()
        {
            using (var context = ZContext.Create())
            {
                var server = new Thread(() => WeatherUpdate.WUServer(context));
                server.Start();
                Thread.Sleep(1000);

                var proxy = new Thread(() => WeatherUpdateProxy.WUProxy());
                proxy.Start();
                Thread.Sleep(1000);

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

        static void MultithreadedRelayTest()
        {
            var thread = new Thread(() => MultithreadedRelay.MTRelay_step3());
            thread.Name = "Relay Entry: Step 3";
            thread.Start();
        }

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

        static void RouterReqTest()
        {
            int numOfWorkers = 10;

            var broker = new Thread(() =>
            {
                RouterReq.RTReq_Broker(numOfWorkers);
            });
            broker.Name = "Broker";
            broker.Start();

            Thread.Sleep(10);

            var workers = new List<Thread>(numOfWorkers);
            for (int i = 0; i < numOfWorkers; i++)
            {
                var worker = new Thread(() =>
                {
                    RouterReq.RTReq_Worker(i);
                });
                worker.Name = "Worker " + i;
                worker.Start();

                Thread.Sleep(10);
            }

            broker.Join();

            foreach (var worker in workers)
            {
                worker.Join();
            }
        }

        static void RouterDealerTest()
        {
            int numOfDealers = 4;

            var broker = new Thread(() =>
            {
                RouterDealer.RTDealer_Broker(numOfDealers);
            });
            broker.Name = "Broker";
            broker.Start();

            Thread.Sleep(10);

            var workers = new List<Thread>(numOfDealers);
            for (int i = 0; i < numOfDealers; i++)
            {
                var worker = new Thread(() =>
                {
                    RouterDealer.RTDealer_Worker(i);
                });
                worker.Name = "Dealer " + i;
                worker.Start();

                Thread.Sleep(10);
            }

            foreach (var worker in workers)
            {
                worker.Join();
                LogService.Debug(string.Format("{0}: done.", worker.Name));
            }

            broker.Join();
            LogService.Debug(string.Format("{0}: done.", broker.Name));
        }
    }
}
