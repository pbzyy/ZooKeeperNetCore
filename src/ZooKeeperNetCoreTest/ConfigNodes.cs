using System.Collections.Generic;

namespace ZooKeeperNetCoreTest
{
    public class Host
    {
        public string Ip { get; set; }
        public int Port { get; set; }
        public int TimeOut { get; set; }
        public string UserName { get; set; }
        public string Password { get; set; }
        public string VirtualHost { get; set; }
    }

    public class ZookeeperNode
    {
        public string Name { get; set; }

        public string Value { get; set; }

        public Host Host { get; set; }

        public List<ZookeeperNode> Childrens { get; set; }
    }
}
