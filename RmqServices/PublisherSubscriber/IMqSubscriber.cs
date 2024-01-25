namespace RmqServices.PublisherSubscriber
{
    public interface IMqSubscriber
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="messageHandler"></param>
        /// /// <exception cref="Models.RmqConnectionException"></exception>
        void StartReceiving(string topic, Action<object?[]> messageHandler);
        /// <summary>
        /// 
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="exchange"></param>
        /// <param name="messageHandler"></param>
        /// <exception cref="Models.RmqConnectionException"></exception>
        void StartReceiving(string topic, string exchange, Action<object?[]> messageHandler);
    }
}