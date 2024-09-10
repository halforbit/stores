using System.IO.Compression;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Reflection.PortableExecutable;
using System.Text.Json;

namespace Halforbit.Stores;

public record Blob<TValue>
{
    public required TValue Value { get; init; }

    public required BlobInfo Info { get; init; }
}

public record BlobInfo
{
}


public record CreateContainerResponse
{
}

public record GetBlobResponse<TKey, TValue>
{
}

public record PutBlobResponse<TKey, TValue>
{
}

public record GetBlobResponse<TValue>
{
    internal TValue? Value { get; init; }
}

public record PutBlobResponse<TValue>
{
}

public interface IPipelineSerializer
{
    void Serialize<TValue>(Stream stream, TValue value);

    TValue Deserialize<TValue>(Stream stream);
}

public class JsonPipelineSerializer : IPipelineSerializer
{
    public void Serialize<TValue>(
        Stream stream,
        TValue value) => System.Text.Json.JsonSerializer.Serialize(stream, value);

    public TValue Deserialize<TValue>(Stream stream) => 
        System.Text.Json.JsonSerializer.Deserialize<TValue>(stream) ?? throw new Exception("Deserialized a null value.");
}

public class GZipPipelineCompressor : IPipelineCompressor
{
    public Stream Compress(
        Stream stream)
    {
        return new GZipStream(stream, CompressionMode.Compress);
    }

    public Stream Decompress(
        Stream stream)
    {
        return new GZipStream(stream, CompressionMode.Decompress);
    }
}

public interface IPipelineCompressor 
{
    public Stream Compress(Stream stream);

    public Stream Decompress(Stream stream);
}

//

public interface IContentSerializer
{
    Task SerializeAsync<T>(PipeWriter writer, T content);
    Task<T> DeserializeAsync<T>(PipeReader reader);
}

public interface ICompressionStrategy
{
    Stream Compress(Stream output);
    Stream Decompress(Stream input);
}

public class JsonSerializerStrategy : IContentSerializer
{
    public async Task SerializeAsync<T>(PipeWriter writer, T content)
    {
        await JsonSerializer.SerializeAsync(writer.AsStream(), content);
        await writer.CompleteAsync();
    }

    public async Task<T> DeserializeAsync<T>(PipeReader reader)
    {
        return await JsonSerializer.DeserializeAsync<T>(reader.AsStream());
    }
}

public class GzipCompressionStrategy : ICompressionStrategy
{
    public Stream Compress(Stream output) => new GZipStream(output, CompressionLevel.Fastest, leaveOpen: true);

    public Stream Decompress(Stream input) => new GZipStream(input, CompressionMode.Decompress, leaveOpen: true);
}

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

    public async Task<T> ReadAndDeserializeAsync<T>(Stream stream)
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

    async Task<T> ReadAndDeserializeAsync<T>(PipeReader reader)
    {
        return await _serializer.DeserializeAsync<T>(reader);
    }
}
