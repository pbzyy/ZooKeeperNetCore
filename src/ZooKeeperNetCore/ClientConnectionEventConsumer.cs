/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

//using log4net;

using System.Threading.Tasks;
using ZooKeeperNet.Logging;

namespace ZooKeeperNet
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Text;
    using System.Collections.Generic;

    public class ClientConnectionEventConsumer : IStartable, IDisposable
    {
        private static readonly IInternalLogger Logger = InternalLoggerFactory.GetInstance<ClientConnectionEventConsumer>();

        private readonly ClientConnection conn;
        //ConcurrentQueue gives us the non-blocking way of processing, it reduced the contention so much
        internal readonly BlockingCollection<ClientConnection.WatcherSetEventPair> waitingEvents = new BlockingCollection<ClientConnection.WatcherSetEventPair>();
        
        /** This is really the queued session state until the event
         * thread actually processes the event and hands it to the watcher.
         * But for all intents and purposes this is the state.
         */
        private volatile KeeperState sessionState = KeeperState.Disconnected;

        public ClientConnectionEventConsumer(ClientConnection conn)
        {
            this.conn = conn;         
        }

        private Task eventTask;

        public void Start()
        {
            eventTask = PollEvents();
        }

        private static async Task ProcessWatcher(IEnumerable<IWatcher> watchers,WatchedEvent watchedEvent)
        {
            foreach (IWatcher watcher in watchers)
            {
                try
                {
                    if (null != watcher)
                    {
                        await watcher.Process(watchedEvent);
                    }
                }
                catch (Exception t)
                {
                    Logger.Error("Error while calling watcher ", t);
                }
            }
        }

        private async Task PollEvents()
        {
            while (!waitingEvents.IsCompleted)
            {
                try
                {
                    ClientConnection.WatcherSetEventPair pair = null;
                    if (waitingEvents.TryTake(out pair, -1))
                    {
                        await ProcessWatcher(pair.Watchers, pair.WatchedEvent);
                    }
                }
                catch (ObjectDisposedException)
                {
                }
                catch (InvalidOperationException)
                {
                }
                catch (OperationCanceledException)
                {
                    //ignored
                }
                catch (Exception t)
                {
                    Logger.Error("Caught unexpected throwable", t);
                }
            }

            Logger.Info("EventThread shut down");
        }

        public void QueueEvent(WatchedEvent @event)
        {
            if (@event.Type == EventType.None && sessionState == @event.State) return;

            if (waitingEvents.IsAddingCompleted)
                throw new InvalidOperationException("consumer has been disposed");
            
            sessionState = @event.State;

            // materialize the watchers based on the event
            var pair = new ClientConnection.WatcherSetEventPair(conn.watcher.Materialize(@event.State, @event.Type,@event.Path), @event);
            // queue the pair (watch set & event) for later processing
            waitingEvents.Add(pair);
        }

        private int isDisposed = 0;
        private void InternalDispose()
        {
            if (Interlocked.CompareExchange(ref isDisposed, 1, 0) == 0)
            {
                waitingEvents.CompleteAdding();
            }
        }

        public void Dispose()
        {
            InternalDispose();
            GC.SuppressFinalize(this);
        }

        ~ClientConnectionEventConsumer()
        {
            InternalDispose();
        }
    }
}
