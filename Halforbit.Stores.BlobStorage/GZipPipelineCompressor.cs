using System.IO.Compression;

namespace Halforbit.Stores;

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
