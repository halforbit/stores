using System.IO.Pipelines;

namespace Halforbit.Stores;

public class ContentPipeline
{
    readonly IContentSerializer _serializer;
    readonly ICompressionStrategy? _compressionStrategy;

    public ContentPipeline(
        IContentSerializer serializer, 
        ICompressionStrategy? compressionStrategy = null)
    {
        _serializer = serializer;
        _compressionStrategy = compressionStrategy;
    }

    public async Task SerializeAndWriteAsync<T>(T content, Stream stream)
    {
        var pipe = new Pipe();
        var writer = pipe.Writer;
        var reader = pipe.Reader;

        var writeTask = WriteSerializedDataAsync(content, writer);

        var readTask = ReadCompressAndWriteToStreamAsync(reader, stream);

        await Task.WhenAll(writeTask, readTask);
    }

    async Task WriteSerializedDataAsync<T>(T content, PipeWriter writer)
    {
        Exception? exception = null;

        try
        {
            await _serializer.SerializeAsync(writer, content);

        }
        catch (Exception ex)
        {
            exception = ex;

            throw;
        }
        finally
        {
            await writer.CompleteAsync(exception);
        }
    }

    async Task ReadCompressAndWriteToStreamAsync(PipeReader reader, Stream stream)
    {
        var outputStream = stream;

        if (_compressionStrategy != null)
        {
            outputStream = _compressionStrategy.Compress(outputStream);
        }

        Exception? exception = null;

        try
        {
            while (true)
            {
                var result = await reader.ReadAsync();
                var buffer = result.Buffer;

                if (buffer.IsEmpty && result.IsCompleted)
                {
                    break;
                }

                try
                {
                    foreach (var segment in buffer)
                    {
                        await outputStream.WriteAsync(segment);
                    }
                }
                finally
                {
                    reader.AdvanceTo(buffer.End);
                }
            }
        }
        catch (Exception ex)
        {
            exception = ex;

            throw;
        }
        finally
        {
            await reader.CompleteAsync(exception);
        }
    }

    public async Task<T?> ReadAndDeserializeAsync<T>(Stream stream)
    {
        var pipe = new Pipe();
        var writer = pipe.Writer;
        var reader = pipe.Reader;

        var readTask = ReadFromStreamAsync(writer, stream);

        var deserializeTask = ReadAndDeserializeAsync<T>(reader);

        await Task.WhenAll(readTask, deserializeTask);
        return deserializeTask.Result;
    }

    async Task ReadFromStreamAsync(PipeWriter writer, Stream stream)
    {
        var inputStream = stream;

        if (_compressionStrategy != null)
        {
            inputStream = _compressionStrategy.Decompress(inputStream);
        }

        Exception? exception = null;

        try
        {
            while (true)
            {
                var memory = writer.GetMemory();
                int bytesRead = await inputStream.ReadAsync(memory);

                if (bytesRead == 0)
                {
                    break;
                }

                writer.Advance(bytesRead);
            }
        }
        catch (Exception ex)
        {
            exception = ex;

            throw;
        }
        finally
        {
            await writer.CompleteAsync(exception);
        }
    }

    async Task<T?> ReadAndDeserializeAsync<T>(PipeReader reader)
    {
        Exception? exception = null;

        try
        {
            return await _serializer.DeserializeAsync<T>(reader);
        }
        catch (Exception ex)
        {
            exception = ex;
            
            throw;
        }
        finally
        {
            await reader.CompleteAsync(exception);
        }
    }
}
