﻿using Lycn.Common.Services;
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
    /// Weather Update using PUB and SUB
    /// </summary>
    public static class WeatherUpdate
    {
        static int numOfCollections = 100;
        public static void WUClient(ZContext context, string address = "tcp://127.0.0.1:5556", int zipcode = 72622)
        {
            Contract.Requires(context != null);

            using (var subscriber = ZSocket.Create(context, ZSocketType.SUB))
            {
                LogService.Debug("{0}: Connecting to {1}...", Thread.CurrentThread.Name, address);
                subscriber.Connect(address);

                LogService.Debug("{0}: Subscribing to zip code {1}...", Thread.CurrentThread.Name, zipcode);
                subscriber.Subscribe(zipcode.ToString());

                int i = 0;
                long total_temperature = 0;
                for (; i < numOfCollections; ++i)
                {
                    using (var replyFrame = subscriber.ReceiveFrame())
                    {
                        string reply = replyFrame.ReadString();

                        LogService.Info("{0}: {1}", Thread.CurrentThread.Name, reply);
                        total_temperature += Convert.ToInt64(reply.Split(' ')[1]);
                    }
                }
                LogService.Info("{0}: Average temperature for zipcode '{1}' was {2}°", Thread.CurrentThread.Name, zipcode, (total_temperature / i));
            }
        }

        public static void WUServer(ZContext context, string address = "tcp://*:5556")
        {
            Contract.Requires(context != null);

            using (var publisher = ZSocket.Create(context, ZSocketType.PUB))
            {
                LogService.Debug("Server: Publisher.Bind'ing on {0}", address);
                publisher.Bind(address);

                LogService.Debug("Press ENTER when the clients are ready...");
                Console.ReadKey(true);

                var rnd = new Random();

                while (Thread.CurrentThread.IsAlive)
                {
                    int zipcode = rnd.Next(99999);
                    int temperature = rnd.Next(-55, +45);

                    var update = string.Format("{0:D5} {1}", zipcode, temperature);
                    using (var updateFrame = new ZFrame(update))
                    {
                        publisher.Send(updateFrame);
                    }
                }
            }
        }
    }
}
