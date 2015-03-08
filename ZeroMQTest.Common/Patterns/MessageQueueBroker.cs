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
    public static class MessageQueueBroker
    {
        public static void MsgQueue(string frontEndBindAddress = "tcp://*:5559", string backendBindAddress = "tcp://*:5560")
        {
            using (var context = ZContext.Create())
            {
                using (var frontend = ZSocket.Create(context, ZSocketType.ROUTER))
                {
                    LogService.Debug(string.Format("{0}: frontend binding on {1}.", Thread.CurrentThread.Name, frontEndBindAddress));
                    frontend.Bind(frontEndBindAddress);

                    using (var backend = ZSocket.Create(context, ZSocketType.DEALER))
                    {
                        LogService.Debug(string.Format("{0}: backend binding on {1}.", Thread.CurrentThread.Name, backendBindAddress));
                        backend.Bind(backendBindAddress);

                        // Start the proxy
                        ZContext.Proxy(frontend, backend);
                    }
                }
            }
        }
    }
}
