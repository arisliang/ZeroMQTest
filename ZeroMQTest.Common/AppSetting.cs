using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ZeroMQTest.Common
{
    public static class AppSetting
    {
        public static int POLLMS { get; private set; }
        public static int WAITINGMS { get; private set; }

        static AppSetting()
        {
            POLLMS = System.Configuration.ConfigurationManager.AppSettings["POLLMS"].ToInt32OrDefault(64);
            WAITINGMS = System.Configuration.ConfigurationManager.AppSettings["WAITINGMS"].ToInt32OrDefault(1);
        }
    }
}
