
using Microsoft.Extensions.ObjectPool;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using RmqServices.Models;

namespace RmqServices.ObjectPools
{
    public class RabbitModelPooledObjectPolicy : IPooledObjectPolicy<IModel>
    {
        private readonly RmqConnectionInfo _connectionInfo;

        private readonly IConnection _connection;

        public RabbitModelPooledObjectPolicy(RmqConnectionInfo connectionInfo, RmqConnectionsCollection connectionsCollection)
        {
            _connectionInfo = connectionInfo;
            _connection = connectionsCollection[_connectionInfo.ConnectionString];
        }

        public IModel Create()
        {
            if (_connection == null)
            {
                throw new RmqConnectionException("Connection with the RabbitMQ server could not be established.", ConnectionError.CouldNotBeEstablished);
            }
            try
            {
                return _connection.CreateModel();
            }
            catch (AlreadyClosedException ace)
            {
                throw new RmqConnectionException("Lost connection with the RabbitMQ server.", ConnectionError.Closed, ace);
            }
        }

        public bool Return(IModel obj)
        {
            if (obj.IsOpen)
            {
                return true;
            }
            else
            {
                obj?.Close();
                return false;
            }
        }
    }
}
