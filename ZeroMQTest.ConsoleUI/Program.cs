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
            LogService.Info("Application started.");

            try
            {
                //HelloWorldTest();
                //WeatherUpdateTest();
                //ParallelTaskTest();
                RequestReplyTest();
            }
            catch (ZException ex)
            {
                LogService.Error(string.Format("{0}\n{1}", ex.Message, ex.StackTrace));
            }

            LogService.Info("Application stopped.");
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
            using (var context = ZContext.Create())
            {
                var broker = new Thread(() => RequestReply.RRBroker(context));
                broker.Name = "Broker";
                broker.Start();

                return;

                int numOfClients = 4;
                int numOfWorkers = 2;
                var clients = new List<Thread>(numOfClients);
                var workers = new List<Thread>(numOfWorkers);

                for (int i = 0; i < numOfClients; i++)
                {
                    var client = new Thread(() => RequestReply.RRClient(context));
                    client.Name = "Client " + i;
                    client.Start();
                }

                for (int i = 0; i < numOfWorkers; i++)
                {
                    var worker = new Thread(() => RequestReply.RRWorker(context));
                    worker.Name = "Worker " + i;
                    worker.Start();
                }

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
        }
    }
}
