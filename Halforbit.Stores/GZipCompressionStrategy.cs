using System.IO.Compression;

namespace Halforbit.Stores;

public class GZipCompressionStrategy : ICompressionStrategy
{
    public Stream Compress(Stream output) => new GZipStream(output, CompressionLevel.Fastest, leaveOpen: true);

    public Stream Decompress(Stream input) => new GZipStream(input, CompressionMode.Decompress, leaveOpen: true);
}
