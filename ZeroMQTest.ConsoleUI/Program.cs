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
                WeatherUpdateTest();
                //ParallelTaskTest();
            }
            catch (ZException ex)
            {
                LogService.Error(string.Format("{0}\n{1}", ex.Message, ex.StackTrace));
            }

            LogService.Info("Application stopped.");
            Console.ReadKey(true);
        }

        /// <summary>
        /// Hello World
        /// </summary>
        static void HelloWorldTest()
        {
            var server = new Thread(() => HelloWorld.HWServer("tcp://*:5555"));
            var client = new Thread(() => HelloWorld.HWClient("tcp://127.0.0.1:5555"));

            server.Start();
            client.Start();

            client.Join();
        }

        static void WeatherUpdateTest()
        {
            using (var context = ZContext.Create())
            {
                var server = new Thread(() => WeatherUpdate.WUServer(context, "tcp://*:5556"));
                server.Start();

                int numOfClients = 6;
                var clients = new List<Thread>();
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

                var workers = new List<Thread>();
                int numOfWorks = 4;
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
    }
}
