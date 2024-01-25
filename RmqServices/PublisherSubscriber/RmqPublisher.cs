using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RmqServices.Models;
using RmqServices.ObjectPools;
using System.Text;

namespace RmqServices.PublisherSubscriber
{
    public class RmqPublisher : IMqPublisher
    {
        private readonly ILogger<RmqPublisher> _logger;
        private readonly ChannelPools _channelPools;
        private readonly string _defaultExchange;
        private bool _disposedValue;

        internal RmqPublisher(ILogger<RmqPublisher> logger, ChannelPools channelPools, string defaultExchange)
        {
            _logger = logger;
            _channelPools = channelPools;
            _defaultExchange = defaultExchange;
        }

        private void PublishMessage(string exchange, RmqMessage message)
        {
            var channel = _channelPools.GetAvailableSendingChannel();
            try
            {
                channel.ExchangeDeclare(exchange: exchange, type: "topic");

                var messageBytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(message));

                channel.BasicPublish(exchange: exchange,
                        routingKey: message.Route,
                        basicProperties: null,
                        body: messageBytes);
                _logger.LogInformation($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}: published message on {exchange} exchange");
            }
            finally
            {
                _channelPools.ReturnSendingChannel(channel);
            }
        }

        public void Publish(string topic, string exchange, params object?[] args)
        {
            var message = new RmqMessage(topic, args!);
            PublishMessage(exchange, message);
        }

        public void Publish(string topic, params object?[] args)
        {
            Publish(topic, _defaultExchange, args);
        }
    }
}
