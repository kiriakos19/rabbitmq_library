using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.ObjectPool;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RmqServices.Models;
using RmqServices.ObjectPools;
using RmqServices.PublisherSubscriber;
using RmqServices.Rpc;
using System.Reflection;
using System.Reflection.Metadata.Ecma335;

namespace RmqServices
{
    public static class DependencyInjectionExtensions
    {
        private static bool _areObjectPoolsRegistered;
        private static bool _isClientFactoryRegistered;

        private static IServiceCollection AddObjectPools(this IServiceCollection services)
        {
            if (!_areObjectPoolsRegistered)
            {
                services.AddSingleton<ObjectPoolProvider, DefaultObjectPoolProvider>();
                services.AddSingleton(s =>
                {
                    var options = s.GetRequiredService<IOptions<RmqConnectionInfo>>();
                    var provider = s.GetRequiredService<ObjectPoolProvider>();
                    var objectPools = new ChannelPools(
                            CreateExpiringModelPool(provider, options.Value, RmqConnectionManager.OutboundConnections),
                            CreateExpiringModelPool(provider, options.Value, RmqConnectionManager.InboundConnections)
                        );
                    return objectPools;
                });
                _areObjectPoolsRegistered = true;
            }
            return services;
        }

        private static IServiceCollection AddClientFactory(this IServiceCollection services)
        {
            services.AddObjectPools();
            if (!_isClientFactoryRegistered)
            {
                services.AddSingleton<RmqClientFactory>();
                _isClientFactoryRegistered = true;
            }
            return services;
        }

        public static IServiceCollection AddRmqServer(this IServiceCollection services, string queueName)
        {
            // Register controllers
            var assembly = Assembly.GetCallingAssembly();
            var controllers = assembly.GetTypes()
                .Where(m => m.GetCustomAttributes(typeof(MqRouteBaseAttribute), false).Length > 0);

            foreach (var controller in controllers)
            {
                services.AddScoped(controller);
            }

            services.AddClientFactory();

            services.AddSingleton(sp => sp.GetRequiredService<RmqClientFactory>().CreateRpcServer(queueName));

            return services;
        }

        public static IServiceCollection AddRmqRpcClient(this IServiceCollection services, string queueName)
        {
            services.AddClientFactory();

            services.AddTransient(sp => sp.GetRequiredService<RmqClientFactory>().CreateRpcClient(queueName));

            return services;
        }

        public static IServiceCollection AddRmqSubscriber(this IServiceCollection services, string defaultExchange)
        {
            services.AddClientFactory();

            services.AddSingleton(sp => sp.GetRequiredService<RmqClientFactory>().CreateTopicSubscriber(defaultExchange));

            return services;
        }

        public static IServiceCollection AddRmqPublisher(this IServiceCollection services, string defaultExchange)
        {
            services.AddClientFactory();

            services.AddTransient(sp => sp.GetRequiredService<RmqClientFactory>().CreateTopicPublisher(defaultExchange));

            return services;
        }

        private static ObjectPool<IModel> CreateExpiringModelPool(ObjectPoolProvider provider, RmqConnectionInfo connectionInfo, RmqConnectionsCollection connections)
        {
            var pool = provider.Create(new RabbitModelPooledObjectPolicy(connectionInfo, connections));
            var expiringPool = new ExpiringObjectPool<IModel>(pool, TimeSpan.FromMinutes(1), channel =>
            {
                try
                {
                    channel.Close();
                }
                catch (ObjectDisposedException) { }
            });
            return expiringPool;
        }
    }
}
