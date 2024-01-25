using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RmqServices.ObjectPools;
using RmqServices.PublisherSubscriber;
using RmqServices.Rpc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RmqServices
{
    internal class RmqClientFactory
    {
        private readonly ChannelPools _channelPools;
        private readonly IServiceProvider _serviceProvider;

        public RmqClientFactory(ChannelPools channelPools, IServiceProvider serviceProvider)
        {
            _channelPools = channelPools;
            _serviceProvider = serviceProvider;
        }

        private string GetCancellationQueueName(string queueName) => queueName + "_cancellations";
        private ILogger<T> GetLogger<T>() => _serviceProvider.GetRequiredService<ILogger<T>>();

        public IMqRpcServer CreateRpcServer(string queueName)
        {
            var logger = GetLogger<RmqRpcServer>();
            var rpcServer = new RmqRpcServer(logger, _serviceProvider, _channelPools, queueName, GetCancellationQueueName(queueName));
            return rpcServer;
        }

        public IMqRpcClient CreateRpcClient(string queueName)
        {
            var logger = GetLogger<RmqRpcClient>();
            var rpcClient = new RmqRpcClient(logger, _channelPools, queueName, GetCancellationQueueName(queueName));
            return rpcClient;
        }

        public IMqPublisher CreateTopicPublisher(string defaultExchange)
        {
            var logger = GetLogger<RmqPublisher>();
            var publisher = new RmqPublisher(logger, _channelPools, defaultExchange);
            return publisher;
        }

        public IMqSubscriber CreateTopicSubscriber(string defaultExchange)
        {
            var logger = GetLogger<RmqSubscriber>();
            var subscriber = new RmqSubscriber(logger, _channelPools, defaultExchange);
            return subscriber;
        }
    }
}
