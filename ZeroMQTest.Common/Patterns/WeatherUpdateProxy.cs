using Lycn.Common.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ZeroMQ;

namespace ZeroMQTest.Common.Patterns
{
    /// <summary>
    /// Protocol not supported.
    /// </summary>
    public static class WeatherUpdateProxy
    {
        public static void WUProxy(string frontendConnectAddess = "tcp://127.0.0.1:5556", string backendBindAddress = "tcp://*:5557")
        {
            using (var context = ZContext.Create())
            {
                using (var frontend = ZSocket.Create(context, ZSocketType.XSUB))
                {
                    // Frontend is where the weather server sits
                    LogService.Debug("{0}: Frontend connecting to {1}", Thread.CurrentThread.Name, frontendConnectAddess);
                    frontend.Connect(frontendConnectAddess);

                    using (var backend = ZSocket.Create(context, ZSocketType.XPUB))
                    {
                        // Backend is our public endpoint for subscribers
                        foreach (IPAddress address in WUProxy_GetPublicIPs())
                        {
                            var tcpAddress = string.Format("tcp://{0}:8100", address);
                            LogService.Debug("{0}: Backend binding on {1}", Thread.CurrentThread.Name, tcpAddress);
                            backend.Bind(tcpAddress);

                            var epgmAddress = string.Format("epgm://{0};239.192.1.1:8100", address);
                            LogService.Debug("{0}: backend binding on {1}", Thread.CurrentThread.Name, epgmAddress);
                            backend.Bind(epgmAddress);  // Protocol not supported.
                        }
                        using (var subscription = ZFrame.Create(1))
                        {
                            subscription.Write(new byte[] { 0x1 }, 0, 1);
                            backend.Send(subscription);
                        }

                        // Run the proxy until the user interrupts us
                        ZContext.Proxy(frontend, backend);
                    }
                }
            }
        }

        static IEnumerable<IPAddress> WUProxy_GetPublicIPs()
        {
            var list = new List<IPAddress>();
            NetworkInterface[] ifaces = NetworkInterface.GetAllNetworkInterfaces();
            foreach (NetworkInterface iface in ifaces)
            {
                if (iface.NetworkInterfaceType == NetworkInterfaceType.Loopback)
                    continue;
                if (iface.OperationalStatus != OperationalStatus.Up)
                    continue;

                var props = iface.GetIPProperties();
                var addresses = props.UnicastAddresses;
                foreach (UnicastIPAddressInformation address in addresses)
                {
                    if (address.Address.AddressFamily == AddressFamily.InterNetwork)
                        list.Add(address.Address);
                    // if (address.Address.AddressFamily == AddressFamily.InterNetworkV6)
                    //    list.Add(address.Address);
                }
            }
            return list;
        }
    }
}
