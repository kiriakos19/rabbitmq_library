using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using RabbitMQ.Client;

namespace RmqServices
{

    public static class RmqConnectionManager
    {
        public static RmqConnectionsCollection InboundConnections { get; }
        public static RmqConnectionsCollection OutboundConnections { get; }
        public static Dictionary<string, ConnectionFactory> ConnectionFactories { get; }

        static RmqConnectionManager()
        {
            InboundConnections = new RmqConnectionsCollection();
            OutboundConnections = new RmqConnectionsCollection();
            ConnectionFactories = new Dictionary<string, ConnectionFactory>();
        }
    }

    public class RmqConnectionsCollection : InternalDataCollectionBase
    {
        private readonly static object _lock = new object();
        private readonly Dictionary<string, IConnection> _connections;
        public RmqConnectionsCollection()
        {
            _connections = new Dictionary<string, IConnection>();
        }

        public IConnection this[string amqpUri]
        {
            get
            {
                lock (_lock)
                {
                    IConnection connection;
                    if (!_connections.ContainsKey(amqpUri))
                    {
                        ConnectionFactory factory;
                        if (!RmqConnectionManager.ConnectionFactories.ContainsKey(amqpUri))
                        {
                            factory = new ConnectionFactory()
                            {
                                Uri = new Uri(amqpUri),
                                RequestedHeartbeat = TimeSpan.FromSeconds(30),
                                UseBackgroundThreadsForIO = true,
                                //DispatchConsumersAsync = true,
                                ClientProvidedName = $"{Assembly.GetEntryAssembly().FullName}"
                            };
                            RmqConnectionManager.ConnectionFactories.Add(amqpUri, factory);
                        }
                        else
                        {
                            factory = RmqConnectionManager.ConnectionFactories[amqpUri];
                        }
                        var callerTid = Thread.CurrentThread.ManagedThreadId;
                        Debug.WriteLine($"Creating connection from thread id: {callerTid}");
                        try
                        {
                            connection = factory.CreateConnection();

                            _connections.Add(amqpUri, connection);
                            //AttachHandlers(amqpUri);

                            return connection;
                        }
                        catch (Exception ex)
                        {
                            Debug.WriteLine("Creating connection exception @ thread id: " + Thread.CurrentThread.ManagedThreadId + "" + Environment.NewLine + ex.ToString());
                            return null;
                        }
                    }
                    connection = _connections[amqpUri];
                    //if (!connection.IsOpen)
                    //{
                    //    //connection.Abort();
                    //    //_connections.Remove(amqpUri);
                    //    connection = null;
                    //}
                    //TODO handle connectionalreadyclosed exception
                    return connection;
                }

            }
        }

        public void RemoveConnection(string amqpuri)
        {
            lock (_lock)
            {
                if (_connections.ContainsKey(amqpuri))
                {
                    var con = _connections[amqpuri];
                    con.Abort();
                    _connections.Remove(amqpuri);
                }
            }
        }


        private void AttachHandlers(string amqpUri)
        {
            var connection = _connections[amqpUri];
            connection.ConnectionShutdown += (sender, args) =>
            {
                var con = _connections[amqpUri];
                _connections.Remove(amqpUri);
                con?.Dispose();
            };
        }

        public override int Count => _connections.Count;
        protected override ArrayList List => new ArrayList(_connections.Values);

    }
}