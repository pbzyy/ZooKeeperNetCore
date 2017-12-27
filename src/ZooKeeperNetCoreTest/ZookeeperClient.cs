using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Org.Apache.Zookeeper.Data;
using ZooKeeperNet;
using ZooKeeperNet.Logging;

namespace ZooKeeperNetCoreTest
{

    public class ZookeeperClientFactory
    {
        private static readonly ConcurrentDictionary<string, Lazy<ZookeeperClient>> ZookeeperClientMap = new ConcurrentDictionary<string, Lazy<ZookeeperClient>>();

        public static ZookeeperClient Get(string hostport)
        {
            var lazyClient = ZookeeperClientMap.GetOrAdd(hostport, (h) =>
            {
                return new Lazy<ZookeeperClient>(() => new ZookeeperClient(h));
            });
            return lazyClient.Value;
        }
    }

    public class ZookeeperClient : IWatcher,IDisposable
    {
        private static readonly IInternalLogger Logger = InternalLoggerFactory.GetInstance<ZookeeperClient>();

        private readonly AutoResetEvent _stateChangedCondition = new AutoResetEvent(false);
        private KeeperState _currentState;
        private TimeSpan _operationTimeOutTimeSpan = TimeSpan.FromMilliseconds(SessionTimeOut * 1.5);

        private const int SessionTimeOut = 10000;

        private const string TimeFormat = "yyyy-MM-dd HH:mm:ss.fff";
        private readonly string _hostport;

        public string Hostport
        {
            get { return _hostport; }
        }

        private string DateNowStr
        {
            get { return DateTime.Now.ToString(TimeFormat); }
        }

        private volatile ZooKeeper _zooKeeper;

        public ZookeeperClient(string hostport, int timeout = SessionTimeOut)
        {
            _hostport = hostport;
            _zooKeeper = new ZooKeeper(_hostport, TimeSpan.FromMilliseconds(timeout), this);
        }

        public Task Process(WatchedEvent @event)
        {
            SetCurrentState(@event.State);

            if (@event.Type == EventType.None)
            {
                Logger.Info($"{DateTime.Now.ToString(TimeFormat)}" + @event);

                switch (@event.State)
                {
                    case KeeperState.SyncConnected:

                        Logger.Info($"{DateNowStr} SyncConnected");

                        _stateChangedCondition.Set();

                        break;
                    case KeeperState.Expired:

                        Logger.Warn($"{DateNowStr} Expired, ReConnect");

                        ReConnect();

                        break;
                    case KeeperState.Disconnected:
                        Logger.Info($"{DateNowStr} Disconnected");
                        break;
                }
            }

            return Task.CompletedTask;
        }

        private void ReConnect()
        {
            ZooKeeper oldzooKeeper = _zooKeeper;
            try
            {
                _zooKeeper = new ZooKeeper(_hostport, TimeSpan.FromMilliseconds(SessionTimeOut), this);
            }
            finally
            {
                oldzooKeeper.Dispose();
            }
        }

        private async Task<T> RetryUntilConnected<T>(Func<Task<T>> callable)
        {
            var operationStartTime = DateTime.Now;
            while (true)
            {

                try
                {
                    return await callable();
                }
                catch (KeeperException.ConnectionLossException)
                {
                    WaitUntilConnected();
                }
                catch (KeeperException.SessionExpiredException)
                {
                    WaitUntilConnected();
                }
                catch (TimeoutException)
                {
                    WaitUntilConnected();
                }

                if (DateTime.Now - operationStartTime > _operationTimeOutTimeSpan)
                {
                    string msg =
                        $"Operation cannot be retried because of retry timeout ({_operationTimeOutTimeSpan.TotalMilliseconds} milli seconds)";

                    throw new TimeoutException(msg);
                }
            }
        }

        private void WaitUntilConnected()
        {
            WaitForKeeperState(KeeperState.SyncConnected, _operationTimeOutTimeSpan);
        }

        private bool WaitForKeeperState(KeeperState states, TimeSpan timeout)
        {
            var stillWaiting = true;
            while (_currentState != states)
            {
                if (!stillWaiting)
                {
                    return false;
                }

                stillWaiting = _stateChangedCondition.WaitOne(timeout);
            }
            return true;
        }

        private void SetCurrentState(KeeperState state)
        {
            lock (this)
            {
                _currentState = state;
            }
        }

        public async Task<bool> IsExist(string path)
        {
            Stat node = await RetryUntilConnected(async () => await _zooKeeper.Exists(path, true));

            if (node == null)
                return false;

            return true;
        }

        public async Task<IEnumerable<string>> GetChilderen(string path, IWatcher watch = null)
        {
            return await RetryUntilConnected(async () => await _zooKeeper.GetChildren(path, watch));
        }

        public async Task<T> GetData<T>(string path, IWatcher watch = null)
        {
            var s = await RetryUntilConnected(async () => await _zooKeeper.GetData(path, watch, null));

            return Deserialize<T>(s);
        }

        public async Task CreateData<T>(string path, T model, int mode = 0)
        {
            await RetryUntilConnected(async () =>
            {
                CreateMode createMode;
                if (mode == 0)
                {
                    createMode = CreateMode.Persistent;
                }
                else if (mode == 1)
                {
                    createMode = CreateMode.Ephemeral;
                }
                else
                {
                    throw new NotSupportedException();
                }
                await _zooKeeper.Create(path, Serialize(model), Ids.OPEN_ACL_UNSAFE, createMode);
                return 1;
            });
        }

        public async Task DeleteData(string path, int version)
        {
            await RetryUntilConnected(async () =>
            {
                await _zooKeeper.Delete(path, version);
                return 1;
            });
        }

        private static byte[] Serialize<T>(T model)
        {
            if (model == null)
                return null;

            if (typeof(T) == typeof(string))
                return model.ToString().GetBytes();

            string str = JsonConvert.SerializeObject(model);
            return str.GetBytes();
        }

        private static T Deserialize<T>(byte[] bytes)
        {
            if (bytes == null) return default(T);

            string node = Encoding.UTF8.GetString(bytes);

            if (typeof(T) == typeof(string))
                return (T)(object)node;

            return JsonConvert.DeserializeObject<T>(node);
        }

        public void Dispose()
        {
            _zooKeeper?.Dispose();
        }
    }
}
