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
    public static class HandlingInterruptSignals
    {
        public static void Interrupt(string bindAddress = "tcp://*:5555")
        {
            string name = "World";

            using (var context = ZContext.Create())
            {
                using (var responder = ZSocket.Create(context, ZSocketType.REP))
                {
                    Console.CancelKeyPress += (s, ea) =>
                        {
                            ea.Cancel = true;
                            context.Shutdown();
                        };

                    responder.Bind(bindAddress);

                    var error = ZError.None;
                    ZFrame request = null;
                    while (Thread.CurrentThread.IsAlive)
                    {
                        if (Console.KeyAvailable)
                        {
                            ConsoleKeyInfo info = Console.ReadKey(true);
                            if (info.Key == ConsoleKey.Escape ||
                                (info.Modifiers == ConsoleModifiers.Control && info.Key == ConsoleKey.C))
                            {
                                context.Shutdown();
                            }
                        }

                        if (null == (request = responder.ReceiveFrame(ZSocketFlags.DontWait, out error)))
                        {
                            if (error == ZError.EAGAIN)
                            {
                                error = ZError.None;
                                Thread.Sleep(512);  // See also the much slower reaction

                                continue;
                            }
                            if (error == ZError.ETERM) break;
                            throw new ZException(error);
                        }

                        using (request)
                        {
                            LogService.Debug("Responder: Received: {0}!", request.ReadString());
                            LogService.Info("Responder: Sending {0}...", name);
                            using (var response = new ZFrame(name))
                            {
                                if (!responder.Send(response, out error))
                                {
                                    if (error == ZError.ETERM) break;   // Interrupted
                                    throw new ZException(error);
                                }
                            }
                        }
                    }

                    if (error == ZError.ETERM)
                    {
                        LogService.Warn("Terminated! You have pressed CTRL+C or ESC.");
                        return;
                    }
                    throw new ZException(error);
                }
            }
        }
    }
}
