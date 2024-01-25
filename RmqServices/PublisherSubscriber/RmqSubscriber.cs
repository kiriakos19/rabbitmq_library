using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RmqServices.Models;
using RmqServices.ObjectPools;
using System.Text;

namespace RmqServices.PublisherSubscriber
{
    public class RmqSubscriber : IDisposable, IMqSubscriber
    {
        private readonly ILogger<RmqSubscriber> _logger;
        private readonly ChannelPools _channelPools;
        private readonly string _defaultExchange;
        private bool _disposedValue;
        private readonly List<ConsumerInfo> _consumers;

        internal RmqSubscriber(ILogger<RmqSubscriber> logger, ChannelPools channelPools, string defaultExchange)
        {
            _logger = logger;
            _channelPools = channelPools;
            _defaultExchange = defaultExchange;
            _consumers = new List<ConsumerInfo>();
        }

        public void StartReceiving(string topic, string exchange, Action<object?[]> messageHandler)
        {
            var channel = _channelPools.GetAvailableReceivingChannel();

            channel.ExchangeDeclare(exchange: exchange, type: "topic");

            var queueName = channel.QueueDeclare().QueueName;
            channel.QueueBind(queue: queueName,
                exchange: exchange,
                routingKey: topic);

            var consumer = new EventingBasicConsumer(channel);
            EventHandler<BasicDeliverEventArgs> receivedHandler = (sender, args) =>
            {
                var serMessage = Encoding.UTF8.GetString(args.Body.ToArray());
                var message = JsonConvert.DeserializeObject<RmqMessage>(serMessage);
                _logger.LogInformation($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}: received message with topic {message?.Route}");
                if (message?.Data != null)
                {
                    Task.Run(() => messageHandler?.Invoke(message.Data));
                }
            };
            consumer.Received += receivedHandler;
            var consumerTag = channel.BasicConsume(queue: queueName,
                autoAck: true,
                consumer: consumer);
            _logger.LogInformation($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}: started receiving on {exchange} exchange with topic {topic}");
            _consumers.Add(new ConsumerInfo(consumerTag, consumer, receivedHandler, channel));
        }

        public void StartReceiving(string topic, Action<object?[]> messageHandler)
        {
            StartReceiving(topic, _defaultExchange, messageHandler);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    foreach (var consumerInfo in _consumers)
                    {
                        consumerInfo.Consumer.Received -= consumerInfo.ReceivedHandler;
                        consumerInfo.Channel.BasicCancel(consumerInfo.ConsumerTag);
                        _channelPools.ReturnReceivingChannel(consumerInfo.Channel);
                    }
                }
                _disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
