using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RmqServices.Models;
using RmqServices.ObjectPools;
using System.Collections.Concurrent;
using System.Text;

namespace RmqServices.Rpc
{
    public class RmqRpcClient : IDisposable, IMqRpcClient
    {
        private bool _disposedValue;

        private readonly ConcurrentDictionary<string, TaskCompletionSource<string>> _callbackMapper = new();
        private readonly ChannelPools _channelPools;
        private readonly string _rpcQueueName;
        private readonly string _rpcCancelationQueueName;
        private readonly string _replyQueueName;
        private readonly ILogger<RmqRpcClient> _logger;
        private readonly ConsumerInfo _consumerInfo;

        internal RmqRpcClient(ILogger<RmqRpcClient> logger, ChannelPools channelPools, string queueName, string cancellationQueueName)
        {
            _logger = logger;
            _channelPools = channelPools;
            _rpcQueueName = queueName;
            _rpcCancelationQueueName = cancellationQueueName;

            // Initialize response receiving channel
            var channel = channelPools.GetAvailableReceivingChannel();
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += Consumer_Received;
            _replyQueueName = channel.QueueDeclare().QueueName;
            var tag = channel.BasicConsume(consumer: consumer,
                                 queue: _replyQueueName,
                                 autoAck: true);
            _consumerInfo = new ConsumerInfo(tag, consumer, Consumer_Received, channel);
            _logger.LogInformation($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}: started listening for responses on {_replyQueueName} queue");
        }

        private void Consumer_Received(object? sender, BasicDeliverEventArgs @event)
        {
            if (!_callbackMapper.TryRemove(@event.BasicProperties.CorrelationId, out var tcs))
                return;
            var body = @event.Body.ToArray();
            var response = Encoding.UTF8.GetString(body);
            tcs.TrySetResult(response);
        }

        public async Task<T> SendRequestAsync<T>(string route, params object[] parameters)
        {
            return await SendRequestAsync<T>(route, default, parameters);
        }

        public async Task<T> SendRequestAsync<T>(string route, CancellationToken cancellationToken, params object[] parameters)
        {
            var response = await GetResponseAsync(route, cancellationToken, parameters);
            var result = (T)response.Data;
            return result;
        }

        public async Task SendRequestAsync(string route, params object[] parameters)
        {
            await SendRequestAsync(route, default, parameters);
        }

        public async Task SendRequestAsync(string route, CancellationToken cancellationToken, params object[] parameters)
        {
            await GetResponseAsync(route, cancellationToken, parameters);
        }

        private async Task<RmqResponse> GetResponseAsync(string route, CancellationToken cancellationToken, params object[] parameters)
        {
            var message = new RmqMessage(route, parameters);
            var correlationId = Guid.NewGuid().ToString();
            var sendingChannel = _channelPools.GetAvailableSendingChannel();

            TaskCompletionSource<string> tcs = new();
            try
            {
                var props = sendingChannel.CreateBasicProperties();
                props.ReplyTo = _replyQueueName;
                props.CorrelationId = correlationId;

                var messageBytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(message));

                cancellationToken.Register(() =>
                {
                    tcs.SetCanceled(cancellationToken);
                    _callbackMapper.TryRemove(correlationId, out _);

                    // Propagate cancellation to server
                    var channel = _channelPools.GetAvailableSendingChannel();
                    try
                    {
                        var basicProps = sendingChannel.CreateBasicProperties();

                        var cancelBytes = Encoding.UTF8.GetBytes(correlationId);
                        channel.BasicPublish(exchange: string.Empty,
                                routingKey: _rpcCancelationQueueName,
                                basicProperties: basicProps,
                                body: cancelBytes);
                    }
                    finally
                    {
                        _channelPools.ReturnSendingChannel(channel);
                    }
                });
                _callbackMapper.TryAdd(correlationId, tcs);

                sendingChannel.BasicPublish(exchange: string.Empty,
                    routingKey: _rpcQueueName,
                    basicProperties: props,
                    body: messageBytes);
                _logger.LogInformation($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}: RPC message with route \"{message.Route}\" sent on {_rpcQueueName} queue");
            }
            finally
            {
                _channelPools.ReturnSendingChannel(sendingChannel);
            }

            var serResponse = await tcs.Task;
            _logger.LogDebug($"Rpc response received on {_replyQueueName} queue");
            var response = JsonConvert.DeserializeObject<RmqResponse>(serResponse, new JsonSerializerSettings()
            {
                PreserveReferencesHandling = PreserveReferencesHandling.All,
                ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
                Formatting = Formatting.Indented,
                TypeNameHandling = TypeNameHandling.All
            })!;
            if (response.HasError)
            {
                throw (Exception)response.Data!;
            }
            return response;
        }

        #region Dispose stuff
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    _consumerInfo.Consumer.Received -= _consumerInfo.ReceivedHandler;
                    _consumerInfo.Channel.BasicCancel(_consumerInfo.ConsumerTag);
                    _channelPools.ReturnReceivingChannel(_consumerInfo.Channel);
                    _logger.LogDebug($"Disposing client owning reply queue {_replyQueueName}");
                }
                _disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }
}
