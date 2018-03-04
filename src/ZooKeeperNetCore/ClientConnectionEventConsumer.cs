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
namespace ZooKeeperNet
{
    using System;
    using System.Threading;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Logging;

    public class ClientConnectionEventConsumer : IStartable, IDisposable
    {
        private static readonly IInternalLogger Logger = InternalLoggerFactory.GetInstance<ClientConnectionEventConsumer>();

        private readonly ClientConnection conn;

        internal readonly AsyncQueue<ClientConnection.WatcherSetEventPair> waitingEvents = new AsyncQueue<ClientConnection.WatcherSetEventPair>();
        
        /** This is really the queued session state until the event
         * thread actually processes the event and hands it to the watcher.
         * But for all intents and purposes this is the state.
         */
        private volatile KeeperState sessionState = KeeperState.Disconnected;

        public ClientConnectionEventConsumer(ClientConnection conn)
        {
            this.conn = conn;         
        }

        private Task pollEventsTask;

        public void Start()
        {
            pollEventsTask = this.PollEvents();
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
            while (!waitingEvents.Cleared)
            {
                try
                {
                    ClientConnection.WatcherSetEventPair pair = await waitingEvents.DequeueAsync();
                    if (pair != null)
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


            sessionState = @event.State;

            // materialize the watchers based on the event
            var pair = new ClientConnection.WatcherSetEventPair(conn.watcher.Materialize(@event.State, @event.Type,@event.Path), @event);
            // queue the pair (watch set & event) for later processing
            waitingEvents.Enqueue(pair);
        }

        private int isDisposed = 0;
        private void InternalDispose()
        {
            if (Interlocked.CompareExchange(ref isDisposed, 1, 0) == 0)
            {
                waitingEvents.Clear();
                pollEventsTask.Dispose();
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
