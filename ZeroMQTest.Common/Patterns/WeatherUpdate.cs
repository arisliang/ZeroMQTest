using Lycn.Common.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ZeroMQ;

namespace ZeroMQTest.Common.Patterns
{
    /// <summary>
    /// Weather Update using PUB and SUB
    /// </summary>
    public static class WeatherUpdate
    {
        public static void WUClient(string address = "tcp://*:5556", int zipcode = 72622)
        {
            using (var context = ZContext.Create())
            {
                using (var subscriber = ZSocket.Create(context, ZSocketType.SUB))
                {
                    LogService.Debug(string.Format("Client: Connecting to {0}...", address));
                    subscriber.Connect(address);

                    LogService.Debug(string.Format("Client: Subscribing to zip code {0}...", zipcode));
                    subscriber.Subscribe(zipcode.ToString());

                    int i = 0;
                    long total_temperature = 0;
                    for (; i < 20; ++i)
                    {
                        using (var replyFrame = subscriber.ReceiveFrame())
                        {
                            string reply = replyFrame.ReadString();

                            LogService.Info(reply);
                            total_temperature += Convert.ToInt64(reply.Split(' ')[1]);
                        }
                    }
                    LogService.Info(string.Format("Average temperature for zipcode '{0}' was {1}°", zipcode, (total_temperature / i)));
                }
            }
        }

        public static void WUServer(string address = "tcp://*:5556")
        {
            using (var context = ZContext.Create())
            {
                using (var publisher = ZSocket.Create(context, ZSocketType.PUB))
                {
                    LogService.Debug(string.Format("Server: Publisher.Bind'ing on {0}", address));
                    publisher.Bind(address);

                    var rnd = new Random();

                    while (true)
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
}
