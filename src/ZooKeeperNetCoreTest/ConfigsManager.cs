using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ZooKeeperNet.Logging;
using Newtonsoft.Json;
using ZooKeeperNet;

namespace ZooKeeperNetCoreTest
{
    public class NodeWatcher : IWatcher
    {
        private const string TimeFormat = "yyyy-MM-dd HH:mm:ss.fff";
        private static readonly IInternalLogger Logger = InternalLoggerFactory.GetInstance<NodeWatcher>();
        readonly IConfigs _configs;

        public NodeWatcher(IConfigs configs)
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

    public interface IConfigs
    {
        Task CreateConfig(string path);

        Task UpdateConfig(string path);

        Task DeleteConfig(string path);

        Task SetNodes(string path);
    }

    public class ConfigsManager: IConfigs
    {
        private static readonly IInternalLogger Logger = InternalLoggerFactory.GetInstance<ConfigsManager>();

        private const string TimeFormat = "yyyy-MM-dd HH:mm:ss.fff";

        private string DateNowStr
        {
            get { return DateTime.Now.ToString(TimeFormat); }
        }

        private ZookeeperClient _zooKeeperClient;

        public ConfigsManager(string hostport)
        {
            _zooKeeperClient = ZookeeperClientFactory.Get(hostport);
        }

        private IWatcher _watcher;
        private readonly object _locker = new object();

        private IWatcher Watcher
        {
            get
            {
                if (_watcher == null)
                {
                    lock (_locker)
                    {
                        if (_watcher == null)
                        {
                            _watcher = new NodeWatcher(this);
                        }
                    }
                }
                return _watcher;
            }
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

                        Logger.Info("all node init");

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
            IEnumerable<string> childrens = await _zooKeeperClient.GetChilderen(path, this.Watcher);
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
                    Value = await _zooKeeperClient.GetData<string>(nodePath, this.Watcher),
                    Childrens = new List<ZookeeperNode>()
                };

                child.Host = SafeDeserializeObject<Host>(child.Value);

                node.Childrens.Add(child);

                await NodesRecursion(child, nodePath);
            }
        }

        public void ConnectZK()
        {
            GetAllNodes().GetAwaiter().GetResult();
        }

        public ZookeeperNode GetConfig(string path)
        {
            var allNodes = _allZookeeperNodes;
            if (allNodes == null)
                throw new ArgumentException("nodes is null");

            if (string.IsNullOrEmpty(path))
                throw new ArgumentException("path cannot be null or empty");

            if (path == "/")
                return allNodes;

            path = path.Trim(new[] { '/' });
            string[] tiers = path.Split('/');

            ZookeeperNode configNode = allNodes;
            for (int i = 0; i < tiers.Length; i++)
            {
                configNode = configNode.Childrens.FirstOrDefault(node => node.Name == tiers[i]);
                if (configNode == null)
                    return null;
            }

            return configNode;
        }

        public async Task<ZookeeperNode> GetConfigAsync(string path)
        {
            var allNodes = await GetAllNodes();
            if (allNodes == null)
                throw new ArgumentException("nodes is null");

            if (string.IsNullOrEmpty(path))
                throw new ArgumentException("path cannot be null or empty");

            if (path == "/")
                return allNodes;

            path = path.Trim(new[] { '/' });
            string[] tiers = path.Split('/');

            ZookeeperNode configNode = allNodes;
            for (int i = 0; i < tiers.Length; i++)
            {
                configNode = configNode.Childrens.FirstOrDefault(node => node.Name == tiers[i]);
                if (configNode == null)
                    return null;
            }

            return configNode;
        }

        public async Task DeleteConfig(string path)
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

        public async Task CreateConfig(string path)
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

            string value = await _zooKeeperClient.GetData<string>(path, this.Watcher);

            ZookeeperNode configNodes = new ZookeeperNode
            {
                Name = tiers[tiers.Length - 1],
                Value = value,
                Childrens = new List<ZookeeperNode>(),
                Host = SafeDeserializeObject<Host>(value)
            };
            configNode.Childrens.Add(configNodes);
        }

        public async Task UpdateConfig(string path)
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

            string value = await _zooKeeperClient.GetData<string>(path,this.Watcher);

            configNode.Value = value;

            configNode.Host = SafeDeserializeObject<Host>(value);        
        }

        public async Task SetNodes(string path)
        {
            var allNodes = await GetAllNodes();
            if (allNodes == null)
                return;

            ZookeeperNode configNode = allNodes;
            if (path != "/")
            {
                if (string.IsNullOrEmpty(path))
                    return;

                string[] tiers = path.Split('/');

                if (tiers.Length == 1)
                    return;

                for (int i = 1; i < tiers.Length; i++)
                {
                    configNode = configNode.Childrens.FirstOrDefault(node => node.Name == tiers[i]);
                    if (configNode == null)
                        return;
                }
            }

            string value = await _zooKeeperClient.GetData<string>(path, this.Watcher);

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

        private static T SafeDeserializeObject<T>(string value) where T : class
        {
            T obj = default(T);
            try
            {
                if (value != null && value.StartsWith("{"))
                    obj = JsonConvert.DeserializeObject<T>(value);
            }
            catch
            {
                // ignored
            }
            return obj;
        }
    }
}
