using System.Reflection;

namespace RmqServices.Rpc
{
    public interface IMqRpcServer
    {
        void AddRouteHandlers(IEnumerable<KeyValuePair<string, MethodInfo>> routeHandlers);
        /// <summary>
        /// Scans Entry Assembly for MQ Route attributes
        /// </summary>
        /// <returns>Route to MethodInfo Dictionary</returns>
        void AddRpcControllersRouteHandlers();
        void StartListening();
    }
}