using Microsoft.Extensions.ObjectPool;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RmqServices.ObjectPools
{
    public class ChannelPools
    {
        private readonly ObjectPool<IModel> _sendingChannels;
        private readonly ObjectPool<IModel> _receivingChannels;
        public ChannelPools(ObjectPool<IModel> sendingChannels, ObjectPool<IModel> receivingChannels)
        {
            _sendingChannels = sendingChannels;
            _receivingChannels = receivingChannels;
        }

        public IModel GetAvailableSendingChannel() => GetAvailableChannel(_sendingChannels);

        public IModel GetAvailableReceivingChannel() => GetAvailableChannel(_receivingChannels);

        public void ReturnSendingChannel(IModel model) => _sendingChannels.Return(model);

        public void ReturnReceivingChannel(IModel model) => _receivingChannels.Return(model);

        private static IModel GetAvailableChannel(ObjectPool<IModel> channels)
        {
            IModel channel = channels.Get();
            while (channel.IsClosed)
            {
                channels.Return(channel);
                channel = channels.Get();
            }
            return channel;
        }
    }
}
