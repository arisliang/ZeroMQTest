using Lycn.Common.Services;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ZeroMQ;

namespace ZeroMQTest.Common.Patterns
{
    public static class RouterDealer
    {
        static int workLength = 5;
        public static void RTDealer_Broker(int numOfWorkers, string brokerBindAddress = "tcp://*:5671")
        {
            using (var context = ZContext.Create())
            {
                using (var broker = ZSocket.Create(context, ZSocketType.ROUTER))
                {
                    LogService.Debug("{0}: binding on {1}", Thread.CurrentThread.Name, brokerBindAddress);
                    broker.Bind(brokerBindAddress);
                    broker.SetOption(ZSocketOption.ROUTER_MANDATORY, 1);

                    var stopwatch = new Stopwatch();
                    stopwatch.Start();

                    // Run for five seconds and then tell workers to end
                    int workers_fired = 0;
                    LogService.Debug("{0}: Just hired {1} worker(s).", Thread.CurrentThread.Name, numOfWorkers);
                    while (true)
                    {
                        // Next message gives us least recently used worker
                        using (ZMessage identity = broker.ReceiveMessage())
                        {
                            //LogService.Debug(string.Format("{0}: worker {1} is free.", Thread.CurrentThread.Name, identity[0].ReadString()));
                            //identity[0].Position = 0;

                            broker.SendMore(identity[0]);   // identity
                            broker.SendMore(new ZFrame());  // empty frame
                            //identity[0].Position = 0;

                            // Encourage workers until it's time to fire them
                            if (stopwatch.Elapsed < TimeSpan.FromSeconds(workLength))
                            {
                                //LogService.Debug(string.Format("{0}: sending work to {1}.", Thread.CurrentThread.Name, identity[0].ReadString()));
                                //identity[0].Position = 0;
                                broker.Send(new ZFrame("Work harder!"));    // data frame
                            }
                            else
                            {
                                //LogService.Debug(string.Format("{0}: no more work for {1}.", Thread.CurrentThread.Name, identity[0].ReadString()));
                                //identity[0].Position = 0;
                                broker.Send(new ZFrame("Fired!"));  // data frame
                                if (++workers_fired == numOfWorkers)
                                {
                                    LogService.Warn("{0}: No more work everybody!", Thread.CurrentThread.Name);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        public static void RTDealer_Worker(int i, string workConnectAddress = "tcp://127.0.0.1:5671")
        {
            using (var context = ZContext.Create())
            {
                using (var worker = ZSocket.Create(context, ZSocketType.DEALER))
                {
                    LogService.Debug("{0}: connecting to {1}.", Thread.CurrentThread.Name, workConnectAddress);
                    worker.IdentityString = "PEER " + i;
                    worker.Connect(workConnectAddress);

                    var rnd = new Random();

                    int total = 0;
                    while (true)
                    {
                        // Tell the broker we're ready for work
                        LogService.Info("{0}: Hey boss, I'm free.", Thread.CurrentThread.Name);
                        worker.SendMore(new ZFrame(worker.Identity));
                        worker.SendMore(new ZFrame());
                        worker.Send(new ZFrame("Hi Boss"));

                        // Get workload from broker, until finished
                        bool finished = false;
                        LogService.Trace("{0}: waiting for reply.", Thread.CurrentThread.Name);
                        using (var message = worker.ReceiveMessage())
                        {
                            string str = message[1].ReadString();
                            LogService.Debug("{0}: Boss said {1}", Thread.CurrentThread.Name, str);
                            finished = (str == "Fired!");
                        }
                        if (finished)
                        {
                            LogService.Warn("{0}: Time to leave.", Thread.CurrentThread.Name);
                            break;
                        }

                        total++;

                        // Do some random work
                        LogService.Info("{0}: doing some work!", Thread.CurrentThread.Name);
                        Thread.Sleep(rnd.Next(0, 2000));
                    }

                    LogService.Info("Completed: PEER {0}, {1} tasks", i, total);
                }
            }
        }
    }
}
