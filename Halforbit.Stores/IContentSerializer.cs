using System.IO.Pipelines;

namespace Halforbit.Stores;

public interface IContentSerializer
{
    Task SerializeAsync<T>(PipeWriter writer, T content);
    Task<T?> DeserializeAsync<T>(PipeReader reader);
}
