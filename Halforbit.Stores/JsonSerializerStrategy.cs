using System.IO.Pipelines;
using System.Text.Json;

namespace Halforbit.Stores;

public class JsonSerializerStrategy : IContentSerializer
{
    public async Task SerializeAsync<T>(PipeWriter writer, T content)
    {
        await JsonSerializer.SerializeAsync(writer.AsStream(), content);
        await writer.CompleteAsync();
    }

    public async Task<T?> DeserializeAsync<T>(PipeReader reader)
    {
        return await JsonSerializer.DeserializeAsync<T>(reader.AsStream());
    }
}
