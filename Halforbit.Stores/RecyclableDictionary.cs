namespace Halforbit.Stores;

public class RecyclableDictionary<TKey, TValue> : 
    IDictionary<TKey, TValue>, 
    IReadOnlyDictionary<TKey, TValue>,
    IDisposable
{
    readonly Dictionary<TKey, TValue> _dictionary;
    
    readonly RecyclableDictionaryManager<TKey, TValue> _manager;
    
    bool _isDisposed;

    internal RecyclableDictionary(
        RecyclableDictionaryManager<TKey, TValue> manager,
        Dictionary<TKey, TValue> dictionary)
    {
        _manager = manager ?? throw new ArgumentNullException(nameof(manager));
        
        _dictionary = dictionary;
    }

    public TValue this[TKey key]
    {
        get => _dictionary[key];
    
        set => _dictionary[key] = value;
    }

    public ICollection<TKey> Keys => _dictionary.Keys;
    
    public ICollection<TValue> Values => _dictionary.Values;
    
    public int Count => _dictionary.Count;
    
    public bool IsReadOnly => ((IDictionary<TKey, TValue>)_dictionary).IsReadOnly;

    IEnumerable<TKey> IReadOnlyDictionary<TKey, TValue>.Keys => _dictionary.Keys;

    IEnumerable<TValue> IReadOnlyDictionary<TKey, TValue>.Values => _dictionary.Values;

    public void Add(TKey key, TValue value) => _dictionary.Add(key, value);
    
    public bool ContainsKey(TKey key) => _dictionary.ContainsKey(key);
    
    public bool Remove(TKey key) => _dictionary.Remove(key);
    
    public bool TryGetValue(TKey key, out TValue value) => _dictionary.TryGetValue(key, out value);

    public void Add(KeyValuePair<TKey, TValue> item) => ((IDictionary<TKey, TValue>)_dictionary).Add(item);
    
    public void Clear() => _dictionary.Clear();
    
    public bool Contains(KeyValuePair<TKey, TValue> item) => _dictionary.Contains(item);
    
    public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex) => ((IDictionary<TKey, TValue>)_dictionary).CopyTo(array, arrayIndex);
    
    public bool Remove(KeyValuePair<TKey, TValue> item) => ((IDictionary<TKey, TValue>)_dictionary).Remove(item);
    
    public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() => _dictionary.GetEnumerator();
    
    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();

    public void Dispose()
    {
        if (!_isDisposed)
        {
            _isDisposed = true;
            
            _dictionary.Clear();
            
            _manager.Return(_dictionary);
        }
    }
}
