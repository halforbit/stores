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

        // Start the serialization process
        var writeTask = WriteSerializedDataAsync(content, writer);

        // Start the compression and network stream write process
        var readTask = ReadCompressAndWriteToStreamAsync(reader, stream);

        // Wait for both tasks to complete
        await Task.WhenAll(writeTask, readTask);
    }

    async Task WriteSerializedDataAsync<T>(T content, PipeWriter writer)
    {
        await _serializer.SerializeAsync(writer, content);
    }

    async Task ReadCompressAndWriteToStreamAsync(PipeReader reader, Stream stream)
    {
        var outputStream = stream;

        // Apply compression if available
        if (_compressionStrategy != null)
        {
            outputStream = _compressionStrategy.Compress(outputStream);
        }

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
                    // Write to the final output stream (compressed or uncompressed)
                    await outputStream.WriteAsync(segment);
                }
            }
            finally
            {
                reader.AdvanceTo(buffer.End);
            }
        }

        await reader.CompleteAsync();
    }

    public async Task<T?> ReadAndDeserializeAsync<T>(Stream stream)
    {
        var pipe = new Pipe();
        var writer = pipe.Writer;
        var reader = pipe.Reader;

        // Start reading from the network stream
        var readTask = ReadFromStreamAsync(writer, stream);

        // Start the deserialization process
        var deserializeTask = ReadAndDeserializeAsync<T>(reader);

        await Task.WhenAll(readTask, deserializeTask);
        return deserializeTask.Result;
    }

    async Task ReadFromStreamAsync(PipeWriter writer, Stream stream)
    {
        var inputStream = stream;

        // Apply decompression if available
        if (_compressionStrategy != null)
        {
            inputStream = _compressionStrategy.Decompress(inputStream);
        }

        while (true)
        {
            var memory = writer.GetMemory();
            int bytesRead = await inputStream.ReadAsync(memory);

            if (bytesRead == 0)
            {
                break;
            }

            // Advance the writer to the position that contains the data read
            writer.Advance(bytesRead);
        }

        await writer.CompleteAsync();
    }

    async Task<T?> ReadAndDeserializeAsync<T>(PipeReader reader)
    {
        return await _serializer.DeserializeAsync<T>(reader);
    }
}
