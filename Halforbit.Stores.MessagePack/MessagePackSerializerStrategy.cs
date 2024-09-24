using MessagePack;
using System.IO.Pipelines;

namespace Halforbit.Stores;

public class MessagePackSerializerStrategy : IContentSerializer
{
    public async Task SerializeAsync<T>(PipeWriter writer, T content)
    {
        await MessagePackSerializer.SerializeAsync(writer.AsStream(), content);
        
        await writer.CompleteAsync();
    }

    public async Task<T> DeserializeAsync<T>(PipeReader reader)
    {
        return await MessagePackSerializer.DeserializeAsync<T>(reader.AsStream());
    }
}
