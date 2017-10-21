using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using ZooKeeperNet.Logging;
using Microsoft.Extensions.Logging.Console;

namespace ZooKeeperNetCoreTest
{
    class Program
    {
        private static readonly IInternalLogger Logger = InternalLoggerFactory.GetInstance<Program>();

        private static Lazy<ConfigsManager> _cmLazy = new Lazy<ConfigsManager>(() => new ConfigsManager("10.1.4.204"));

        static void Main(string[] args)
        {
            InternalLoggerFactory.DefaultFactory.AddProvider(new ConsoleLoggerProvider((s, level) => true, false));

            TT:

            ZKClientTest();

            ConfigsManagerTest();
 
            goto TT;
        }

        /// <summary>
        /// ZKClientTest
        /// </summary>
        private static void ZKClientTest()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            const int c = 10000;
            CountdownEvent k = new CountdownEvent(c);
            var cm = _cmLazy.Value;
            Parallel.For(0, c, (i) =>
            {
                var task = cm.GetData<string>("/sz");
                task.ContinueWith(n =>
                {
                    if (n.IsFaulted)
                    {
                        if (i % 5000 == 0)
                        {
                            Console.WriteLine($"{i} {n.Exception}");
                        }
                    }
                    k.Signal(1);
                });
            });
            k.Wait();
            Logger.Info("ZKClientTest " + sw.ElapsedMilliseconds);
        }

        /// <summary>
        /// ConfigsManagerTest
        /// </summary>
        private static void ConfigsManagerTest()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            const int c = 10000;
            CountdownEvent k = new CountdownEvent(c);
            var cm = _cmLazy.Value;
            Parallel.For(0, c, (i) =>
            {
                var task = cm.GetConfig("/sz");
                task.ContinueWith(n =>
                {
                    //Logger.Info(JsonConvert.SerializeObject(n.Result.Value) + "   " + i);
                    k.Signal(1);
                });
            });
            k.Wait();
            Logger.Info("ConfigsManagerTest " + sw.ElapsedMilliseconds);
        }

        /// <summary>
        /// HttpTest
        /// </summary>
        private static void HttpTest()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            const int c = 30000;
            CountdownEvent k = new CountdownEvent(c);
            const string requestUrl = "http://192.168.100.6:85/sz/Basic/CityRouteRequest";
            Parallel.For(0, c, (i) =>
            {
                var task = HttpRequestHelper.DoGetAsync(requestUrl);
                task.ContinueWith(n => { k.Signal(1); });
            });

            k.Wait();
            Logger.Info("HttpTest " + sw.ElapsedMilliseconds);
        }
    }
}
