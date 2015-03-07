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
                HelloWorldTest();
            }
            catch (ZException ex)
            {
                LogService.Error(string.Format("{0}\n{1}", ex.Message, ex.StackTrace));
            }

            LogService.Info("Application stopped.");
        }

        /// <summary>
        /// Hello World
        /// </summary>
        static void HelloWorldTest()
        {
            var client = new Thread(HelloWorld.HWClient);
            var server = new Thread(HelloWorld.HWServer);

            client.Start();
            server.Start();

            client.Join();
        }
    }
}
