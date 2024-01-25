using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RmqServices.Models;
using RmqServices.ObjectPools;
using System.Collections.Concurrent;
using System.Reflection;
using System.Text;

namespace RmqServices.Rpc
{
    public class RmqRpcServer : IDisposable, IMqRpcServer
    {
        private readonly ILogger<RmqRpcServer> _logger;
        private readonly IServiceProvider _serviceProvider;
        private readonly ChannelPools _channelPools;
        private readonly string _queueName;
        private readonly string _cancellationQueueName;
        private bool _disposedValue;

        private readonly Dictionary<string, MethodInfo> _routeHandlers;
        private readonly ConcurrentDictionary<string, CancellationTokenSource> _runningTaskTokenSources;

        private readonly List<ConsumerInfo> _consumers;

        internal RmqRpcServer(ILogger<RmqRpcServer> logger, IServiceProvider serviceProvider, ChannelPools channelPools, string queueName, string cancellationQueueName)
        {
            _routeHandlers = new Dictionary<string, MethodInfo>();
            _logger = logger;
            _serviceProvider = serviceProvider;
            _channelPools = channelPools;
            _runningTaskTokenSources = new ConcurrentDictionary<string, CancellationTokenSource>();
            _consumers = new List<ConsumerInfo>();
            _queueName = queueName;
            _cancellationQueueName = cancellationQueueName;
        }

        public void StartListening()
        {
            // Initialize main request receiving channel
            var receivingChannel = _channelPools.GetAvailableReceivingChannel();
            InitializeReceivingChannel(receivingChannel, _queueName, Consumer_Received);
            _logger.LogInformation($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}: started listening on {_queueName}");
            // Initialize task cancellation request receiving channel
            var cancellationReceivingChannel = _channelPools.GetAvailableReceivingChannel();
            InitializeReceivingChannel(cancellationReceivingChannel, _cancellationQueueName, CancellationConsumer_Received);
            _logger.LogInformation($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}: started listening on {_cancellationQueueName}");
        }

        public void AddRpcControllersRouteHandlers()
        {
            var dic = new Dictionary<string, MethodInfo>();
            var assembly = Assembly.GetEntryAssembly()!;
            var methods = assembly.GetTypes()
                  .SelectMany(t => t.GetMethods())
                  .Where(m => m.GetCustomAttributes(typeof(MqRouteAttribute), false).Length > 0)
                  .ToArray();

            foreach (var mi in methods)
            {
                var type = mi.DeclaringType;
                var routeBaseAttribute = type?.GetCustomAttribute<MqRouteBaseAttribute>(inherit: false);
                string routeBase = "";
                if (routeBaseAttribute != null)
                {
                    routeBase = routeBaseAttribute.RouteBase;
                }

                foreach (var ca in mi.GetCustomAttributes<MqRouteAttribute>(false))
                {
                    var route = ca.Route;
                    var fullRoute = $"{routeBase}/{route}".Trim('/');
                    dic.Add(fullRoute, mi);
                }
            }
            AddRouteHandlers(dic.ToList());
        }

        public void AddRouteHandlers(IEnumerable<KeyValuePair<string, MethodInfo>> routeHandlers)
        {
            foreach (var kvp in routeHandlers)
            {
                _routeHandlers[kvp.Key] = kvp.Value;
            }
        }

        private void InitializeReceivingChannel(IModel channel, string queueName, EventHandler<BasicDeliverEventArgs> receivedHandler)
        {
            channel.QueueDeclare(queue: queueName,
                                  durable: false,
                                  exclusive: false,
                                  autoDelete: true,
                                  arguments: null);
            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
            var consumer = new EventingBasicConsumer(channel);
            var consumerTag = channel.BasicConsume(queue: queueName,
                                  autoAck: true,
                                  consumer: consumer);
            consumer.Received += receivedHandler;

            _consumers.Add(new ConsumerInfo(consumerTag, consumer, receivedHandler, channel));
        }

        private void CancellationConsumer_Received(object? sender, BasicDeliverEventArgs e)
        {
            var body = e.Body.ToArray();
            var correlationId = Encoding.UTF8.GetString(body);

            // Cancels running operation if requested by client service
            if (_runningTaskTokenSources.TryRemove(correlationId, out var tokenSource))
            {
                tokenSource.Cancel();
                _logger.LogInformation($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}: received cancellation request with Id {correlationId}");
            }
        }

        /// <summary>
        /// Creates response and publishes it replyQueue
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        /// <exception cref="ArgumentNullException"></exception>
        private void Consumer_Received(object? sender, BasicDeliverEventArgs e)
        {
            RmqResponse? response = null;
            var body = e.Body.ToArray();
            var props = e.BasicProperties;

            Task.Run(async () =>
            {

                var sendingChannel = _channelPools.GetAvailableSendingChannel();
                var replyProps = sendingChannel.CreateBasicProperties();
                replyProps.CorrelationId = props.CorrelationId;
                try
                {
                    var serliazedMessage = Encoding.UTF8.GetString(body);
                    var message = JsonConvert.DeserializeObject<RmqMessage>(serliazedMessage);
                    _logger.LogInformation($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}: received rpc message with route {message.Route}");
                    if (message == null)
                    {
                        throw new ArgumentNullException(nameof(message), "Deserialized message is null.");
                    }

                    // Create cancellation token source to be signaled by client service
                    var tokenSource = new CancellationTokenSource();
                    _runningTaskTokenSources[props.CorrelationId] = tokenSource;

                    var result = await CreateResponseAsync(message, tokenSource.Token);
                    response = new RmqResponse(result);
                }
                catch (Exception ex)
                {
                    var wrapperException = new Exception("MQ Server threw an exception while processing the request", ex);

                    response = new RmqResponse(wrapperException, true);
                }
                finally
                {
                    _runningTaskTokenSources.TryRemove(props.CorrelationId, out _);

                    // Serialize response and publish
                    var serResponse = JsonConvert.SerializeObject(response, new JsonSerializerSettings()
                    {
                        PreserveReferencesHandling = PreserveReferencesHandling.All,
                        ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
                        Formatting = Formatting.Indented,
                        TypeNameHandling = TypeNameHandling.All
                    });
                    var responseBytes = Encoding.UTF8.GetBytes(serResponse);

                    sendingChannel.BasicPublish(exchange: string.Empty,
                                         routingKey: props.ReplyTo,
                                         basicProperties: replyProps,
                                         body: responseBytes);
                    _logger.LogDebug($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}: responded to {props.ReplyTo}");
                    // TODO handle closed channel
                    _channelPools.ReturnSendingChannel(sendingChannel);
                }
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="message"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        private async Task<object?> CreateResponseAsync(RmqMessage message, CancellationToken cancellationToken)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }
            if (!_routeHandlers.TryGetValue(message.Route.Trim('/'), out var method))
            {
                _logger.LogWarning($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}: received route not registered");
                throw new ArgumentException($"{message.Route} not registered on the RPC server");
            }
            using var scope = _serviceProvider.CreateScope();
            var controllerType = method.DeclaringType!;
            var service = scope.ServiceProvider.GetRequiredService(controllerType);
            var methodParams = method.GetParameters();

            // Convert parameters to correct Type
            for (int i = 0; i < (message.Data?.Length ?? 0); i++)
            {
                if (i >= methodParams.Count()) break;

                var paramInfo = methodParams[i];
                var param = message.Data![i];
                if (param == null) continue;

                var dataType = param.GetType();
                if (dataType == typeof(JObject))
                {
                    var jarg = (JObject)param;

                    object? arg = jarg.ToObject(paramInfo.ParameterType);

                    message.Data[i] = arg;
                }
                else if (dataType == typeof(JArray))
                {
                    var jarg = (JArray)param;

                    object? arg = jarg.ToObject(paramInfo.ParameterType);

                    message.Data[i] = arg;
                }
                else
                {
                    message.Data[i] = Convert.ChangeType(message.Data[i], paramInfo.ParameterType);
                }
            }

            return await AwaitableSafeExecute(method, service, cancellationToken, message.Data!);

        }

        private async Task<object?> AwaitableSafeExecute(MethodInfo methodInfo, object instance, CancellationToken cancellationToken, params object?[] arguments)
        {
            // https://stackoverflow.com/a/50205807
            var isAwaitable = methodInfo.ReturnType.GetMethod(nameof(Task.GetAwaiter)) != null;

            object? invokeResult = null;
            if (isAwaitable)
            {
                //pass cancelattion token to args if required
                if (methodInfo.GetParameters().Length > arguments.Length && methodInfo.GetParameters().Last().ParameterType == typeof(CancellationToken))
                {
                    var args = new List<object?>(arguments)
                    {
                        cancellationToken
                    };
                    arguments = args.ToArray();
                }
                if (methodInfo.ReturnType.IsGenericType)
                {
                    var res = methodInfo.Invoke(instance, arguments);
                    if (res != null)
                    {
                        invokeResult = await (dynamic)res;
                    }
                }
                else
                {
                    var res = methodInfo.Invoke(instance, arguments);
                    if (res != null)
                    {
                        await (Task)res;
                    }
                }
            }
            else
            {
                if (methodInfo.ReturnType == typeof(void))
                {
                    methodInfo.Invoke(instance, arguments);
                }
                else
                {
                    invokeResult = methodInfo.Invoke(instance, arguments);
                }
            }

            return invokeResult;
        }

        #region Dispose stuff
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

                    _logger.LogDebug($"Disposing server owning queue {_queueName} and {_cancellationQueueName}");
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