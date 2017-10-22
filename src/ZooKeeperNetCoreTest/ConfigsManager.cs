using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ZooKeeperNet.Logging;
using Newtonsoft.Json;
using Org.Apache.Zookeeper.Data;
using ZooKeeperNet;

namespace ZooKeeperNetCoreTest
{
    public class ConfigsManager : IWatcher
    {
        private static readonly IInternalLogger Logger = InternalLoggerFactory.GetInstance<ConfigsManager>();

        private readonly AutoResetEvent _stateChangedCondition = new AutoResetEvent(false);
        private KeeperState _currentState;
        private TimeSpan _operationTimeOutTimeSpan = TimeSpan.FromMilliseconds(SessionTimeOut * 1.5);

#if DEBUG
        private const int SessionTimeOut = 10000;
#else
        private const int SessionTimeOut = 5000;
#endif

        private const string TimeFormat = "yyyy-MM-dd HH:mm:ss.fff";
        private readonly string _hostport;

        private string DateNowStr
        {
            get { return DateTime.Now.ToString(TimeFormat); }
        }

        private volatile bool _signal;
        private volatile ZooKeeper _zooKeeper;

        public ConfigsManager(string hostport, int timeout = SessionTimeOut)
        {
            _hostport = hostport;
            _zooKeeper = new ZooKeeper(_hostport, TimeSpan.FromMilliseconds(timeout), this);
        }

        public async Task Process(WatchedEvent @event)
        {
            SetCurrentState(@event.State);

            if (@event.Type == EventType.None)
            {
                Logger.Info($"{DateTime.Now.ToString(TimeFormat)}" + @event);

                switch (@event.State)
                {
                    case KeeperState.SyncConnected:
                        _stateChangedCondition.Set();

                        if (_signal)
                            await GetAllNodes();

                        _signal = true;

                        Logger.Info($"{DateNowStr} SyncConnected");
                        break;
                    case KeeperState.Expired:
                        ReConnect();

                        Logger.Warn($"{DateNowStr} Expired, ReConnect");
                        break;
                    case KeeperState.Disconnected:
                        Logger.Info($"{DateNowStr} Disconnected");
                        break;
                }
            }
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

        private IWatcher _watcher;

        private IWatcher Watcher
        {
            get
            {
                if (_watcher == null)
                    _watcher = new NodeWatcher(this);

                return _watcher;
            }
        }

        private class NodeWatcher : IWatcher
        {
            readonly ConfigsManager _configs;

            public NodeWatcher(ConfigsManager configs)
            {
                this._configs = configs;
            }

            public async Task Process(WatchedEvent @event)
            {
                if (@event?.Path == null) return;

                if (@event.Type == EventType.None)
                    return;

                Logger.Info($"{DateTime.Now.ToString(TimeFormat)}" + @event);

                switch (@event.Type)
                {
                    case EventType.NodeDataChanged:
                        await _configs.UpdateConfig(@event.Path);
                        break;
                    case EventType.NodeDeleted:
                        await _configs.DeleteConfig(@event.Path);
                        break;
                    case EventType.NodeCreated:
                        await _configs.CreateConfig(@event.Path);
                        break;
                    case EventType.NodeChildrenChanged:
                        await _configs.SetNodes(@event.Path);
                        break;
                }
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
            return await RetryUntilConnected(async ()=> await _zooKeeper.GetChildren(path, watch));
        }

        public async Task<T> GetData<T>(string path, IWatcher watch = null)
        {
            var s = await RetryUntilConnected(async () => await _zooKeeper.GetData(path, watch, null));

            return Deserialize<T>(s);
        }

        public async Task CreateData<T>(string path, T model)
        {
            await RetryUntilConnected(async () =>
            {
                await _zooKeeper.Create(path, Serialize(model), Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);
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

        private readonly SemaphoreSlim _semaphoreSlim = new SemaphoreSlim(1, 1);
        private volatile ZookeeperNode _allZookeeperNodes;

        private async Task<ZookeeperNode> GetAllNodes()
        {
            while (true)
            {
                try
                {
                    if (_allZookeeperNodes != null)
                        return _allZookeeperNodes;

                    await _semaphoreSlim.WaitAsync();
                    try
                    {
                        if (_allZookeeperNodes != null)
                            return _allZookeeperNodes;

                        var node = new ZookeeperNode
                        {
                            Name = "/",
                            Value = "",
                            Childrens = new List<ZookeeperNode>()
                        };

                        await NodesRecursion(node, "/");

                        if (node.Childrens.Count == 0)
                        {
                            throw new Exception("get none node");
                        }

                        _allZookeeperNodes = node;
                        return _allZookeeperNodes;
                    }
                    finally
                    {
                        _semaphoreSlim.Release();
                    }
                }
                catch (Exception ex)
                {
                    Logger.Info($"{DateNowStr} failed load all nodes,again. {ex.Message}");

                    await Task.Delay(4000);
                }
            }          
        }

        private async Task NodesRecursion(ZookeeperNode node, string path)
        {
            IEnumerable<string> childrens = await GetChilderen(path, this.Watcher);
            if (childrens == null || !childrens.Any())
                return;

            foreach (string str in childrens)
            {
                string nodePath = path;
                if (!path.EndsWith("/"))
                    nodePath += "/";

                nodePath += str;

                ZookeeperNode child = new ZookeeperNode
                {
                    Name = str,
                    Value = await GetData<string>(nodePath, this.Watcher),
                    Childrens = new List<ZookeeperNode>()
                };

                try
                {
                    if (child.Value != null && child.Value.StartsWith("{"))
                        child.Host = JsonConvert.DeserializeObject<Host>(child.Value);
                }
                catch
                {
                    // ignored
                }

                node.Childrens.Add(child);

                await NodesRecursion(child, nodePath);
            }
        }

        public async Task<ZookeeperNode> GetConfig(string path)
        {
            var allNodes = await GetAllNodes();
            if (allNodes == null)
                throw new ArgumentException("nodes is null");

            if (string.IsNullOrEmpty(path))
                throw new ArgumentException("path cannot be null or empty");

            path = path.Trim(new[] { '/' });
            string[] tiers = path.Split('/');

            ZookeeperNode returnNode = allNodes;
            for (int i = 0; i < tiers.Length; i++)
            {
                var temp = tiers[i];
                returnNode = returnNode.Childrens.FirstOrDefault(node => node.Name == temp);
            }


            return returnNode;

        }

        async Task DeleteConfig(string path)
        {
            var allNodes = await GetAllNodes();
            if (allNodes == null)
                return;

            if (string.IsNullOrEmpty(path))
                return; 

            string[] tiers = path.Split('/');

            if (tiers.Length == 1)
                return; 

            ZookeeperNode configNode = allNodes;

            for (int i = 1; i < tiers.Length - 1; i++)
            {
                configNode = configNode.Childrens.FirstOrDefault(node => node.Name == tiers[i]);
                if (configNode == null)
                    return;
            }

            ZookeeperNode leafNode = configNode.Childrens.FirstOrDefault(x => x.Name == tiers[tiers.Length - 1]);
            configNode.Childrens.Remove(leafNode);
        }

        async Task CreateConfig(string path)
        {
            var allNodes = await GetAllNodes();
            if (allNodes == null)
                return;

            if (string.IsNullOrEmpty(path))
                return;

            string[] tiers = path.Split('/');

            if (tiers.Length == 1)
                return;

            ZookeeperNode configNode = allNodes;

            for (int i = 1; i < tiers.Length - 1; i++)
            {
                configNode = configNode.Childrens.FirstOrDefault(node => node.Name == tiers[i]);
                if (configNode == null)
                    return;
            }

            string value = await GetData<string>(path, this.Watcher);

            ZookeeperNode configNodes = new ZookeeperNode
            {
                Name = tiers[tiers.Length - 1],
                Value = value,
                Childrens = new List<ZookeeperNode>()
            };

            try
            {
                if (value.StartsWith("{"))
                    configNodes.Host = JsonConvert.DeserializeObject<Host>(value);
            }
            catch
            {
                // ignored
            }

            configNode.Childrens.Add(configNodes);
        }

        async Task UpdateConfig(string path)
        {
            var allNodes = await GetAllNodes();
            if (allNodes == null)
                return;

            if (string.IsNullOrEmpty(path))
                return;

            string[] tiers = path.Split('/');

            if (tiers.Length == 1)
                return;

            ZookeeperNode configNode = allNodes;

            for (int i = 1; i < tiers.Length; i++)
            {
                configNode = configNode.Childrens.FirstOrDefault(node => node.Name == tiers[i]);
                if (configNode == null)
                    return;
            }

            string value = await GetData<string>(path,this.Watcher);

            configNode.Value = value;

            try
            {
                if (value.StartsWith("{"))
                    configNode.Host = JsonConvert.DeserializeObject<Host>(value);
            }
            catch
            {
                // ignored
            }
        }


        async Task SetNodes(string path)
        {
            var allNodes = await GetAllNodes();
            if (allNodes == null)
                return;

            if (string.IsNullOrEmpty(path))
                return;

            string[] tiers = path.Split('/');

            if (tiers.Length == 1)
                return;

            ZookeeperNode configNode = allNodes;

            for (int i = 1; i < tiers.Length; i++)
            {
                configNode = configNode.Childrens.FirstOrDefault(node => node.Name == tiers[i]);
                if (configNode == null)
                    return;
            }

            string value = await GetData<string>(path, this.Watcher);

            ZookeeperNode newNode = new ZookeeperNode()
            {
                Host = configNode.Host,
                Name = configNode.Name,
                Value = value,
                Childrens = new List<ZookeeperNode>()
            };

            await NodesRecursion(newNode, path);

            configNode.Childrens = newNode.Childrens;
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
    }
}
