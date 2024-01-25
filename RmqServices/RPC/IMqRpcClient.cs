namespace RmqServices.Rpc
{
    public interface IMqRpcClient
    {
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="route"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        /// <exception cref="Models.RmqConnectionException"></exception>
        Task<T> SendRequestAsync<T>(string route, params object[] parameters);
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="route"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        /// <exception cref="Models.RmqConnectionException"></exception>
        Task<T> SendRequestAsync<T>(string route, CancellationToken cancellationToken, params object[] parameters);
        /// <summary>
        /// 
        /// </summary>
        /// <param name="route"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        /// <exception cref="Models.RmqConnectionException"></exception>
        Task SendRequestAsync(string route, params object[] parameters);
        /// <summary>
        /// 
        /// </summary>
        /// <param name="route"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        /// <exception cref="Models.RmqConnectionException"></exception>
        Task SendRequestAsync(string route, CancellationToken cancellationToken, params object[] parameters);
    }
}