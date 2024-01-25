using Microsoft.Extensions.ObjectPool;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace RmqServices.ObjectPools
{
    internal class ExpiringObjectPool<T> : ObjectPool<T> where T : class
    {
        private readonly ObjectPool<T> _source;
        private readonly TimeSpan _timeToLive;
        private readonly Action<T> _onExpire;
        private readonly ConcurrentDictionary<T, Timer> _expirationTimers;

        public ExpiringObjectPool(ObjectPool<T> source, TimeSpan timeToLive, Action<T> onExpire)
        {
            _source = source;
            _timeToLive = timeToLive;
            _onExpire = onExpire;
            _expirationTimers = new ConcurrentDictionary<T, Timer>();
        }

        public override T Get()
        {
            var obj = _source.Get();
            TryRemoveExpirationTimer(obj);
            return obj;
        }

        public override void Return(T obj)
        {
            var timer = new Timer(Expire, obj, _timeToLive, Timeout.InfiniteTimeSpan);
            _expirationTimers.TryAdd(obj, timer);
            _source.Return(obj);
        }

        private void TryRemoveExpirationTimer(T obj)
        {
            if (_expirationTimers.TryRemove(obj, out var timer))
            {
                try
                {
                    _ = timer.Change(Timeout.Infinite, Timeout.Infinite);
                    timer.Dispose();
                }
                catch (ObjectDisposedException) { }
            }
        }

        private void Expire(object? state)
        {
            if (state == null) return;
            var obj = (T)state;
            TryRemoveExpirationTimer(obj);
            _onExpire(obj);
        }
    }
}
