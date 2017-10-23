using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using ZooKeeperNet.IO;
using ZooKeeperNet.Logging;

namespace ZooKeeperNet
{
    using System.IO;
    using System.Linq;
    using System.Net.Sockets;
    using System.Text;
    using System.Threading;
    using Org.Apache.Jute;
    using Org.Apache.Zookeeper.Proto;
    using System.Collections.Generic;

    public class ClientConnectionRequestProducer : IStartable, IDisposable
    {
        private static readonly IInternalLogger Logger = InternalLoggerFactory.GetInstance<ClientConnectionRequestProducer>();

        private const string RETRY_CONN_MSG = ", closing socket connection and attempting reconnect";

        private readonly ClientConnection conn;
        private readonly ZooKeeper zooKeeper;

        private readonly ConcurrentQueue<Packet> pendingQueue = new ConcurrentQueue<Packet>();
        private readonly LinkedList<Packet> outgoingQueue = new LinkedList<Packet>();
        private readonly AutoResetEvent packetAre = new AutoResetEvent(false);

        public int PendingQueueCount
        {
            get
            {
                return pendingQueue.Count;
            }
        }
        public int OutgoingQueueCount { get { return outgoingQueue.Count; } }

        private Socket client;
        private readonly Random random = new Random();
        private int initialized;
        internal long lastZxid;
        private long lastPingSentTs;
        internal int xid = 1;
        private volatile bool closing;

        private byte[] incomingBuffer = new byte[4];
        internal int sentCount;
        internal int recvCount;
        internal int negotiatedSessionTimeout;

        private ZooKeeperEndpoints zkEndpoints;

        private int connectionClosed;
        public bool IsConnectionClosedByServer
        {
            get
            {
                return Interlocked.CompareExchange(ref connectionClosed, 0, 0) == 1;
            }
            private set
            {
                Interlocked.Exchange(ref connectionClosed, value ? 1 : 0);
            }
        }

        public ClientConnectionRequestProducer(ClientConnection conn)
        {
            this.conn = conn;
            zooKeeper = conn.zooKeeper;
            zkEndpoints = new ZooKeeperEndpoints(conn.serverAddrs);
        }

        protected int Xid
        {
            get { return Interlocked.Increment(ref xid); }
        }

        private Task sendTask;

        public void Start()
        {
            sendTask = SendRequests();
        }

        public Packet QueuePacket(RequestHeader h, ReplyHeader r, IRecord request, IRecord response, string clientPath, string serverPath, ZooKeeper.WatchRegistration watchRegistration)
        {
            if (h.Type != (int)OpCode.Ping && h.Type != (int)OpCode.Auth)
                h.Xid = Xid;

            Packet p = new Packet(h, r, request, response, null, watchRegistration, clientPath, serverPath);
    
            if (!zooKeeper.State.IsAlive() || closing || Interlocked.CompareExchange(ref isDisposed, 0, 0) == 1)
            {
                if(Logger.DebugEnabled)
                    Logger.Debug("Connection closing. Sending ConLossPacket. IsAlive: {}, closing: {}", zooKeeper.State.IsAlive(), closing);
                ConLossPacket(p);
            }
            else
            {
                if (h.Type == (int)OpCode.CloseSession)
                    closing = true;
                // enqueue the packet when zookeeper is connected
                addPacketLast(p);
            }
            return p;
        }

        private void addPacketFirst(Packet p)
        {
            outgoingQueue.AddFirst(p);
            
        }
        private void addPacketLast(Packet p)
        {
            lock (outgoingQueue)
            {
                outgoingQueue.AddLast(p);
            }
            packetAre.Set();
        }

        private async Task SendRequests()
        {
            var now = GetDateTimeUtcNow();
            DateTime lastSend = now;
            Packet packet = null;

            while (zooKeeper.State.IsAlive())
            {
                try
                {
                    now = GetDateTimeUtcNow();
                    if (client == null || (!client.Connected || zooKeeper.State == ZooKeeper.States.NOT_CONNECTED))
                    {
                        // don't re-establish connection if we are closing
                        if(conn.IsClosed || closing)
                            break;

                        await StartConnect().ConfigureAwait(false); ;

                        now = GetDateTimeUtcNow();
                        lastSend = now;
                        lastHeard = now;
                    }
                    if (zooKeeper.State == ZooKeeper.States.CONNECTED)
                    {
                        TimeSpan idleRec = now - lastHeard;
                        var ts = conn.readTimeout - idleRec;
                        if (ts.TotalMilliseconds <= 0)
                        {
                            string warnInfo =
                                  $"Client session timed out, have not heard from server in {idleRec.TotalMilliseconds}ms for sessionid 0x{conn.SessionId.ToString("X")}";

                            throw new SessionTimeoutException(warnInfo);
                        }
                    }

                    TimeSpan idleSend = now - lastSend;
                    if (zooKeeper.State == ZooKeeper.States.CONNECTED)
                    {
                        TimeSpan timeToNextPing = new TimeSpan(0, 0, 0, 0, Convert.ToInt32(conn.readTimeout.TotalMilliseconds / 2 - idleSend.TotalMilliseconds));
                        if (timeToNextPing <= TimeSpan.Zero)
                            SendPing();
                    }
                    // Everything below and until we get back to the select is
                    // non blocking, so time is effectively a constant. That is
                    // Why we just have to do this once, here                    

                    packet = null;
                    lock (outgoingQueue)
                    {
                        if(!outgoingQueue.IsEmpty())
                        {
                            packet = outgoingQueue.First();
                            outgoingQueue.RemoveFirst();
                        }
                    }
                    if (packet != null)
                    {
                        // We have something to send so it's the same
                        // as if we do the send now.                        
                        await DoSend(packet).ConfigureAwait(false); ;
                        lastSend = DateTime.UtcNow;
                        packet = null;
                    }
                    else
                        packetAre.WaitOne(TimeSpan.FromMilliseconds(1));
                }
                catch (Exception e)
                {
                    if (conn.IsClosed || closing)
                    {
                        if (Logger.DebugEnabled)
                        {
                            // closing so this is expected
                            Logger.Debug("An exception was thrown while closing send thread for session 0x{} : {}", conn.SessionId.ToString("X"), e.Message);
                        }
                        break;
                    }
                    else
                    {
                        // this is ugly, you have a better way speak up
                        if (e is KeeperException.SessionExpiredException)
                            Logger.Warn("{}, closing socket connection", e.Message);
                        else if (e is SessionTimeoutException)
                            Logger.Warn("{}{}", e.Message, RETRY_CONN_MSG);
                        else if (e is System.IO.EndOfStreamException)
                            Logger.Warn("{}{}", e.Message, RETRY_CONN_MSG);
                        else
                            Logger.Warn("Session 0x{} for server {}, unexpected error{}, detail:{}-{}", conn.SessionId.ToString("X"), null, RETRY_CONN_MSG, e.Message, e.StackTrace);
                        // a safe-net ...there's a packet about to send when an exception happen
                        if (packet != null)
                            ConLossPacket(packet);
                        // clean up any queued packet
                        Cleanup();
                        if(zooKeeper.State.IsAlive())
                        {
                            conn.consumer.QueueEvent(new WatchedEvent(KeeperState.Disconnected, EventType.None, null));
                        }
                    }
                }
            }
            
            // safe-net to ensure everything is clean up properly
            Cleanup();

            // i don't think this is necessary, when we reached this block ....the state is surely not alive
            if (zooKeeper.State.IsAlive())
                conn.consumer.QueueEvent(new WatchedEvent(KeeperState.Disconnected, EventType.None, null));

            if (Logger.DebugEnabled)
                Logger.Debug("SendThread exitedloop.");
        }

        private static DateTime GetDateTimeUtcNow()
        {
            DateTime now = DateTime.UtcNow;
            return now;
        }

        private void Cleanup(Socket tcpClient)
        {
            if (tcpClient != null)
            {
                try
                {
                    // close the connection
                    if (tcpClient.Connected)
                    {
                        tcpClient.Shutdown(SocketShutdown.Both);
                    }
                    tcpClient.Close();
                }
                catch (IOException e)
                {
                    if (Logger.DebugEnabled)
                        Logger.Debug("Ignoring exception during channel close", e);
                }
            }

            lock (outgoingQueue)
            {
                foreach (var packet in outgoingQueue)
                {
                    ConLossPacket(packet);
                }
                outgoingQueue.Clear();
            }

            Packet pack;
            while (pendingQueue.TryDequeue(out pack))
                ConLossPacket(pack);
        }

        private void Cleanup()
        {
            Cleanup(client);
            client = null;
        }

        private bool firstConnect = true;

        private async Task StartConnect()
        {
            zooKeeper.State = ZooKeeper.States.CONNECTING;
            Socket tempClient = null;
            do
            {
                if (zkEndpoints.EndPointID != -1)
                {
                    await Task.Delay(new TimeSpan(0, 0, 0, 0, random.Next(0, 50)));

                    if (!zkEndpoints.IsNextEndPointAvailable)
                    {
                        await Task.Delay(1000);
                    }
                }

                //advance through available connections;
                zkEndpoints.GetNextAvailableEndpoint();

                if (!firstConnect)
                {
                    Cleanup(null);
                }

                firstConnect = false;
                Logger.Info("{} Opening socket connection to server {}", DateTimeUtcNowStr, zkEndpoints.CurrentEndPoint.ServerAddress);
                tempClient = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                tempClient.LingerState = new LingerOption(false, 0);
                tempClient.NoDelay = true;
                tempClient.Blocking = false;

                Interlocked.Exchange(ref initialized, 0);
                IsConnectionClosedByServer = false;

                try
                {
                    await tempClient.ConnectAsync(zkEndpoints.CurrentEndPoint.ServerAddress.Address,
                        zkEndpoints.CurrentEndPoint.ServerAddress.Port);

                    Logger.Info("{} socket Connectd  to server {}", DateTimeUtcNowStr, zkEndpoints.CurrentEndPoint.ServerAddress);

                    break;
                }
                catch (Exception ex)
                {
                    if (ex is SocketException || ex is TimeoutException)
                    {
                        Cleanup(tempClient);
                        tempClient = null;
                        zkEndpoints.CurrentEndPoint.SetAsFailure();

                        Logger.Warn(string.Format("Failed to connect to {0}:{1}.",
                            zkEndpoints.CurrentEndPoint.ServerAddress.Address.ToString(),
                            zkEndpoints.CurrentEndPoint.ServerAddress.Port.ToString()));
                    }
                    else
                    {
                        throw;
                    }
                }
            }
            while(zkEndpoints.IsNextEndPointAvailable);

            if (tempClient == null)
            {
                throw KeeperException.Create(KeeperException.Code.CONNECTIONLOSS);
            }

            client = tempClient;
            client.BeginReceive(incomingBuffer, 0, incomingBuffer.Length, SocketFlags.None, ReceiveAsynch, incomingBuffer);
            PrimeConnection();
        }

        private static string DateTimeUtcNowStr
        {
            get { return GetDateTimeUtcNow().ToString("yyyy-MM-dd HH:mm:ss.fff"); }
        }

        private byte[] juteBuffer;

        private int currentLen;

        private DateTime lastHeard;

        /// <summary>
        /// process the receiving mechanism in asynchronous manner.
        /// Zookeeper server sent data in two main parts
        /// part(1) -> contain the length of the part(2)
        /// part(2) -> contain the interest information
        /// 
        /// Part(2) may deliver in two or more segments so it is important to 
        /// handle this situation properly
        /// </summary>
        /// <param name="ar">The asynchronous result</param>
        private void ReceiveAsynch(IAsyncResult ar)
        {
            if (Interlocked.CompareExchange(ref isDisposed, 0, 0) == 1)
            {
                return;
            }
            int len = 0;
            try
            {
                if (client == null)
                    return;

                try
                {
                    len = client.EndReceive(ar);
                }
                catch(Exception ex)
                {
                    Logger.Error("EndReceive Error len:{} ex:{} ", len, ex);
                }
                if (len == 0) //server closed the connection...
                {
                    if (Logger.DebugEnabled)
                        Logger.Debug("TcpClient connection lost.");

                    if(zooKeeper.State == ZooKeeper.States.CONNECTING)
                        zkEndpoints.CurrentEndPoint.SetAsFailure();

                    zooKeeper.State = ZooKeeper.States.NOT_CONNECTED;
                    IsConnectionClosedByServer = true;
                    return;
                }
                byte[] bData = (byte[])ar.AsyncState;
                recvCount++;

                if (bData == incomingBuffer) // if bData is incoming then surely it is a length information
                {                    
                    currentLen = 0;
                    juteBuffer = null;
                    // get the length information from the stream
                    juteBuffer = new byte[ReadLength(bData)];

                    // try getting other info from the stream
                    client.BeginReceive(juteBuffer, 0, juteBuffer.Length, SocketFlags.None, ReceiveAsynch, juteBuffer);
                }
                else // not an incoming buffer then it is surely a zookeeper process information
                {
                    if (Interlocked.CompareExchange(ref initialized,1,0) == 0)
                    {
                        // haven't been initialized so read the authentication negotiation result
                        ReadConnectResult(bData);

                        lastHeard = GetDateTimeUtcNow();

                        // reading length information
                        client.BeginReceive(incomingBuffer, 0, incomingBuffer.Length, SocketFlags.None, ReceiveAsynch, incomingBuffer);
                    }
                    else
                    {
                        currentLen += len;
                        if (juteBuffer.Length > currentLen) // stream haven't been completed so read any left bytes
                        {
                            client.BeginReceive(juteBuffer, currentLen, juteBuffer.Length - currentLen, SocketFlags.None, ReceiveAsynch, juteBuffer);
                        }
                        else
                        {
                            // stream is complete so read the response
                            ReadResponse(bData);

                            lastHeard = GetDateTimeUtcNow();

                            // everything is fine, now read the stream again for length information
                            client.BeginReceive(incomingBuffer, 0, incomingBuffer.Length, SocketFlags.None, ReceiveAsynch, incomingBuffer);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex);
            }
        }

        private void PrimeConnection()
        {
            Logger.Info("Socket connection established to {}, initiating session", client.RemoteEndPoint);
            ConnectRequest conReq = new ConnectRequest(0, lastZxid, Convert.ToInt32(conn.SessionTimeout.TotalMilliseconds), conn.SessionId, conn.SessionPassword);

            lock (outgoingQueue)
            {
                if (!ClientConnection.DisableAutoWatchReset && (!zooKeeper.DataWatches.IsEmpty() || !zooKeeper.ExistWatches.IsEmpty() || !zooKeeper.ChildWatches.IsEmpty()))
                {
                    var sw = new SetWatches(lastZxid, zooKeeper.DataWatches, zooKeeper.ExistWatches, zooKeeper.ChildWatches);
                    var h = new RequestHeader();
                    h.Type = (int)OpCode.SetWatches;
                    h.Xid = -8;
                    Packet packet = new Packet(h, new ReplyHeader(), sw, null, null, null, null, null);

                    addPacketFirst(packet);
                }

                foreach (ClientConnection.AuthData id in conn.authInfo)
                {
                    var packet = new Packet(new RequestHeader(-4, (int) OpCode.Auth), null, new AuthPacket(0, id.Scheme, id.GetData()), null, null, null, null, null);
                    addPacketFirst(packet);
                }

                addPacketFirst(new Packet(null, null, conReq, null, null, null, null, null));
                
            }
            packetAre.Set();

            Logger.Info("Session establishment request sent on {}", client.RemoteEndPoint);
        }

        private void SendPing()
        {
            lastPingSentTs = DateTime.UtcNow.Ticks();
            RequestHeader h = new RequestHeader(-2, (int)OpCode.Ping);
            conn.QueuePacket(h, null, null, null, null, null, null, null, null);
        }
        
        /// <summary>
        /// send packet to server        
        /// there's posibility when server closed the socket and client try to send some packet, when this happen it will throw exception
        /// the exception is either IOException, NullReferenceException and/or ObjectDisposedException
        /// so it is mandatory to catch these excepetions
        /// </summary>
        /// <param name="packet">The packet to send</param>
        private async Task DoSend(Packet packet)
        {
            if (packet.header != null
                && packet.header.Type != (int)OpCode.Ping
                && packet.header.Type != (int)OpCode.Auth)
            {
                pendingQueue.Enqueue(packet);
            }
   
            await client.SendAsync(new ArraySegment<byte>(packet.data, 0, packet.data.Length), SocketFlags.None);
            sentCount++;
        }

        private static int ReadLength(byte[] content)
        {
            using (EndianBinaryReader reader = new EndianBinaryReader(EndianBitConverter.Big, new MemoryStream(content), Encoding.UTF8))
            {
                int len = reader.ReadInt32();
                if (len < 0 || len >= ClientConnection.MaximumPacketLength)
                    throw new IOException(new StringBuilder("Packet len ").Append(len).Append("is out of range!").ToString());
                return len;
            }
        }

        private void ReadConnectResult(byte[] content)
        {
            using (var reader = new EndianBinaryReader(EndianBitConverter.Big, new MemoryStream(content), Encoding.UTF8))
            {
                zkEndpoints.CurrentEndPoint.SetAsSuccess();
                BinaryInputArchive bbia = BinaryInputArchive.GetArchive(reader);
                ConnectResponse conRsp = new ConnectResponse();
                conRsp.Deserialize(bbia, "connect");
                negotiatedSessionTimeout = conRsp.TimeOut;
                if (negotiatedSessionTimeout <= 0)
                {
                    zooKeeper.State = ZooKeeper.States.CLOSED;
                    conn.consumer.QueueEvent(new WatchedEvent(KeeperState.Expired, EventType.None, null));
                    throw new SessionExpiredException(new StringBuilder().AppendFormat("Unable to reconnect to ZooKeeper service, session 0x{0:X} has expired", conn.SessionId).ToString());
                }
                conn.readTimeout = new TimeSpan(0, 0, 0, 0, negotiatedSessionTimeout * 2 / 3);
                conn.SessionId = conRsp.SessionId;
                conn.SessionPassword = conRsp.Passwd;
                zooKeeper.State = ZooKeeper.States.CONNECTED;
                Logger.Info("Session establishment complete on server {}, negotiated timeout = {}", conn.SessionId.ToString("X"), negotiatedSessionTimeout);
                conn.consumer.QueueEvent(new WatchedEvent(KeeperState.SyncConnected, EventType.None, null));
            }
        }

        private void ReadResponse(byte[] content)
        {
            using (var reader = new EndianBinaryReader(EndianBitConverter.Big, new MemoryStream(content), Encoding.UTF8))
            {
                BinaryInputArchive bbia = BinaryInputArchive.GetArchive(reader);
                ReplyHeader replyHdr = new ReplyHeader();
                replyHdr.Deserialize(bbia, "header");
                if (replyHdr.Xid == -2)
                {
                    // -2 is the xid for pings
                    if (Logger.DebugEnabled)
                    {
                        var ts = new TimeSpan(DateTime.UtcNow.Ticks() - lastPingSentTs);
                        Logger.Debug("Got ping response for sessionid: 0x{} after {}ms", conn.SessionId.ToString("X"), ts.TotalMilliseconds);
                    }

                    return;
                }
                if (replyHdr.Xid == -4)
                {
                    // -4 is the xid for AuthPacket
                    // TODO: process AuthPacket here
                    if (Logger.DebugEnabled)
                        Logger.Debug("Got auth sessionid:0x{}", conn.SessionId.ToString("X"));
                    return;
                }
                if (replyHdr.Xid == -1)
                {
                    // -1 means notification
                    if (Logger.DebugEnabled)
                        Logger.Debug("Got notification sessionid:0x{}", conn.SessionId.ToString("X"));

                    WatcherEvent @event = new WatcherEvent();
                    @event.Deserialize(bbia, "response");

                    // convert from a server path to a client path
                    if (conn.ChrootPath != null)
                    {
                        string serverPath = @event.Path;
                        var i = String.Compare(serverPath, conn.ChrootPath, StringComparison.Ordinal);
                        if (i == 0)
                            @event.Path = PathUtils.PathSeparator;
                        else
                            @event.Path = serverPath.Substring(conn.ChrootPath.Length);
                    }

                    WatchedEvent we = new WatchedEvent(@event);
                    if (Logger.DebugEnabled)
                        Logger.Debug("Got {} for sessionid 0x{}", we, conn.SessionId.ToString("X"));

                    conn.consumer.QueueEvent(we);
                    return;
                }
                Packet packet;
                /*
                 * Since requests are processed in order, we better get a response
                 * to the first request!
                 */
                if (pendingQueue.TryDequeue(out packet))
                {
                    try
                    {
                        if (packet.header.Xid != replyHdr.Xid)
                        {
                            packet.replyHeader.Err = (int)KeeperException.Code.CONNECTIONLOSS;
                            throw new IOException($"Xid out of order. Got {replyHdr.Xid} expected {packet.header.Xid}");                  
                        }

                        packet.replyHeader.Xid = replyHdr.Xid;
                        packet.replyHeader.Err = replyHdr.Err;
                        packet.replyHeader.Zxid = replyHdr.Zxid;
                        if (replyHdr.Zxid > 0)
                            lastZxid = replyHdr.Zxid;

                        if (packet.response != null && replyHdr.Err == 0)
                            packet.response.Deserialize(bbia, "response");

                    }
                    finally
                    {
                        FinishPacket(packet);
                    }
                }
                else
                {
                    throw new IOException(new StringBuilder("Nothing in the queue, but got ").Append(replyHdr.Xid).ToString());
                }
            }
        }        

        private void ConLossPacket(Packet p)
        {
            if (p.replyHeader == null) return;

            string state = zooKeeper.State.State;
            if (state == ZooKeeper.States.AUTH_FAILED.State)
                p.replyHeader.Err = (int)KeeperException.Code.AUTHFAILED;
            else if (state == ZooKeeper.States.CLOSED.State)
                p.replyHeader.Err = (int)KeeperException.Code.SESSIONEXPIRED;
            else
                p.replyHeader.Err = (int)KeeperException.Code.CONNECTIONLOSS;

            FinishPacket(p);
        }

        private static string ToHexString(byte[] bytes)
        {
            string hexString = string.Empty;
            if (bytes != null)
            {
                StringBuilder strB = new StringBuilder();
                for (int i = 0; i < bytes.Length; i++)
                {
                    strB.Append(bytes[i].ToString("X2"));
                }
                hexString = strB.ToString();

            }
            return hexString;
        }

        private static void FinishPacket(Packet p)
        {
            if (p.watchRegistration != null)
                p.watchRegistration.Register(p.replyHeader.Err);

            p.SetFinished();
        }

        private int isDisposed = 0;

        private void InternalDispose()
        {
            if (Interlocked.CompareExchange(ref isDisposed, 1, 0) == 0)
            {
                zooKeeper.State = ZooKeeper.States.CLOSED;
                
                incomingBuffer = juteBuffer = null;
            }
        }

        public void Dispose()
        {
            InternalDispose();
            GC.SuppressFinalize(this);
        }

        ~ClientConnectionRequestProducer()
        {
            InternalDispose();
        }
    }
}
