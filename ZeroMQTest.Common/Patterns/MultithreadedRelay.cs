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
    public static class MultithreadedRelay
    {
        public static void MTRelay_step3()
        {
            // Bind inproc socket before starting step2
            using (var context = ZContext.Create())
            {
                using (var receiver = ZSocket.Create(context, ZSocketType.PAIR))
                {
                    receiver.Bind("inproc://step3");

                    var thread = new Thread(() => MTRelay_step2(context));
                    thread.Name = "Step 3";
                    thread.Start();

                    // Wait for signal
                    receiver.ReceiveFrame();

                    LogService.Info(string.Format("{0}: Ready, Test successful!", Thread.CurrentThread.Name));
                }
            }
        }

        private static void MTRelay_step2(ZContext context)
        {
            // Bind inproc socket before starting step1
            using (var receiver = ZSocket.Create(context, ZSocketType.PAIR))
            {
                receiver.Bind("inproc://step2");

                var thread = new Thread(() => MTRelay_step1(context));
                thread.Name = "Step 2";
                thread.Start();

                // Wait for signal and pass it on
                receiver.ReceiveFrame();
            }

            // Connect to step3 and tell it we're ready
            using (var xmitter = ZSocket.Create(context, ZSocketType.PAIR))
            {
                xmitter.Connect("inproc://step3");

                LogService.Info(string.Format("{0}: Ready, signaling step 3", Thread.CurrentThread.Name));
                xmitter.Send(new ZFrame("READY"));
            }
        }

        private static void MTRelay_step1(ZContext context)
        {
            // Connect to step2 and tell it we're ready
            using (var xmitter = ZSocket.Create(context, ZSocketType.PAIR))
            {
                xmitter.Connect("inproc://step2");

                LogService.Info(string.Format("{0}: Ready, signaling step 2", Thread.CurrentThread.Name));
                xmitter.Send(new ZFrame("READY"));
            }
        }
    }
}
