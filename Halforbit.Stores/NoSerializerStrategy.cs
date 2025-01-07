using System.IO.Pipelines;

namespace Halforbit.Stores;

public class NoSerializerStrategy : IContentSerializer
{
    public async Task SerializeAsync<T>(PipeWriter writer, T content)
    {
        if (typeof(T) == typeof(byte[]))
        {
            await writer.WriteAsync((byte[])(object)content!);
        }
        else if (typeof(T) == typeof(ReadOnlyMemory<byte>))
        {
            await writer.WriteAsync((ReadOnlyMemory<byte>)(object)content!);
        }
        else if (typeof(T) == typeof(Stream))
        {
            var stream = (Stream)(object)content!;
            await stream.CopyToAsync(writer);
            await writer.FlushAsync();
        }
        else
        {
            throw new NotSupportedException($"Type {typeof(T)} is not supported for serialization.");
        }
    }

    public async Task<T?> DeserializeAsync<T>(PipeReader reader)
    {
        if (typeof(T) == typeof(byte[]))
        {
            using (var ms = new MemoryStream())
            {
                while (true)
                {
                    var result = await reader.ReadAsync();
                    var buffer = result.Buffer;

                    foreach (var segment in buffer)
                    {
                        ms.Write(segment.Span);
                    }

                    reader.AdvanceTo(buffer.End);

                    if (result.IsCompleted)
                    {
                        break;
                    }
                }

                return (T)(object)ms.ToArray();
            }
        }
        else if (typeof(T) == typeof(ReadOnlyMemory<byte>))
        {
            var byteArray = await DeserializeAsync<byte[]>(reader);
            return (T)(object)new ReadOnlyMemory<byte>(byteArray);
        }
        else if (typeof(T) == typeof(Stream))
        {
            var byteArray = await DeserializeAsync<byte[]>(reader);
            return (T)(object)new MemoryStream(byteArray);
        }
        else
        {
            throw new NotSupportedException($"Type {typeof(T)} is not supported for deserialization.");
        }
    }
}