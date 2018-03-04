using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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

        private static Lazy<ConfigsManager> _cmLazy = new Lazy<ConfigsManager>(() => new ConfigsManager("10.1.62.66"));

        static void Main(string[] args)
        {
            InternalLoggerFactory.DefaultFactory.AddProvider(new ConsoleLoggerProvider((s, level) => true, false));

            while (true)
            {
                //HttpTest();

                ZKClientTest();

                ConfigsManagerSyncTest();

                Console.ReadLine();
            }
        }

        /// <summary>
        /// ConfigsManagerSyncTest
        /// </summary>
        private static void ConfigsManagerSyncTest()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            var cm = _cmLazy.Value;
            cm.ConnectZK();

            const int c = 10000;
            CountdownEvent k = new CountdownEvent(c);

            Parallel.For(0, c, (i) =>
            {
                var node = cm.GetConfig("/sz");
                k.Signal(1);
            });
            k.Wait();
            Console.WriteLine("ConfigsManagerTest " + sw.ElapsedMilliseconds);
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
            var zookeeperClient = ZookeeperClientFactory.Get("10.1.62.66");
            Parallel.For(0, c, (i) =>
            {
                var task = zookeeperClient.GetData<string>("/sz");
                task.ContinueWith(n =>
                {
                    if (n.IsFaulted)
                    {
                        Console.WriteLine($"{i} {n.Exception}");
                    }
                    k.Signal(1);
                });
            });
            k.Wait();
            Console.WriteLine("ZKClientTest " + sw.ElapsedMilliseconds);
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
                var task = cm.GetConfigAsync("/sz");
                task.ContinueWith(n =>
                {
                    k.Signal(1);
                });
            });
            k.Wait();
            Console.WriteLine("ConfigsManagerTest " + sw.ElapsedMilliseconds);
        }

        /// <summary>
        /// HttpTest
        /// </summary>
        private static void HttpTest()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            const int c = 1000;
            CountdownEvent k = new CountdownEvent(c);
            const string requestUrl = "http://10.1.4.204:9566/sz/Basic/CityRouteRequest";
            Parallel.For(0, c, (i) =>
            {
                var task = HttpRequestHelper.DoGetAsync(requestUrl);
                task.ContinueWith(n =>
                {
                    if (n.IsFaulted)
                    {
                        Console.WriteLine(n.Exception);
                    }
                    k.Signal(1);
                });
            });

            k.Wait();
            Console.WriteLine("HttpTest " + sw.ElapsedMilliseconds);
        }
    }
   
}
