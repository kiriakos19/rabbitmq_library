namespace RmqServices.PublisherSubscriber
{
    public interface IMqPublisher
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="exchange"></param>
        /// <param name="message"></param>
        /// <exception cref="Models.RmqConnectionException"></exception>
        void Publish(string topic, params object?[] args);
        /// <summary>
        /// 
        /// </summary>
        /// <param name="exchange"></param>
        /// <param name="message"></param>
        /// <exception cref="Models.RmqConnectionException"></exception>
        void Publish(string topic, string exchange, params object?[] args);
    }
}