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
    public static class MultithreadedService
    {
        public static void MTServer(string clientsBindAddress = "tcp://*:5555",
            string workersBindAddress = "inproc://workers")
        {
            using (var context = ZContext.Create())
            {
                using (var clients = ZSocket.Create(context, ZSocketType.ROUTER))
                {
                    clients.Bind(clientsBindAddress);

                    // Launch pool of worker threads
                    int numOfWorkers = 5;
                    for (int i = 0; i < numOfWorkers; ++i)
                    {
                        var thread = new Thread(() => MTServer_Worker(context));
                        thread.Name = string.Format("[Worker {0}]", i);
                        thread.Start();
                    }

                    using (var workers = ZSocket.Create(context, ZSocketType.DEALER))
                    {
                        workers.Bind(workersBindAddress);

                        ZContext.Proxy(clients, workers);
                    }
                }
            }
        }

        static void MTServer_Worker(ZContext context, string workerConnectAddress = "inproc://workers")
        {
            using (var server = ZSocket.Create(context, ZSocketType.REP))
            {
                server.Connect(workerConnectAddress);

                while (true)
                {
                    using (ZFrame frame = server.ReceiveFrame())
                    {
                        LogService.Debug("{0}: Worker received {1}", Thread.CurrentThread.Name, frame.ReadString());

                        // Do some 'work'
                        Thread.Sleep(1);

                        // Send reply back to client
                        string replyText = "World";
                        LogService.Debug("{0}: Sending {1}", Thread.CurrentThread.Name, replyText);
                        server.Send(new ZFrame(replyText));
                    }
                }
            }
        }
    }
}
