using System.Collections.Concurrent;

namespace Halforbit.Stores;

public class RecyclableDictionaryManager<TKey, TValue>
{
    readonly ConcurrentBag<Dictionary<TKey, TValue>> _pool;

    public RecyclableDictionaryManager()
    {
        _pool = new ConcurrentBag<Dictionary<TKey, TValue>>();
    }

    public RecyclableDictionary<TKey, TValue> Get()
    {
        if (_pool.TryTake(out var dictionary))
        {
            return new RecyclableDictionary<TKey, TValue>(
                this, 
                dictionary);
        }

        return new RecyclableDictionary<TKey, TValue>(
            this, 
            new Dictionary<TKey, TValue>());
    }

    internal void Return(Dictionary<TKey, TValue> dictionary)
    {
        if (_pool.Count < 100)
        {
            _pool.Add(dictionary);
        }
    }
}
