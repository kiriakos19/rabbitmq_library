using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RmqServices.Models
{
    public class ConsumerInfo
    {
        public ConsumerInfo(string consumerTag, EventingBasicConsumer consumer, EventHandler<BasicDeliverEventArgs> receivedHandler, IModel channel)
        {
            ConsumerTag = consumerTag;
            Consumer = consumer;
            ReceivedHandler = receivedHandler;
            Channel = channel;
        }

        public string ConsumerTag { get; set; }
        public EventingBasicConsumer Consumer { get; set; }
        public EventHandler<BasicDeliverEventArgs> ReceivedHandler { get; set; }
        public IModel Channel { get; set; }
    }
}
