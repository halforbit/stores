namespace Halforbit.Stores;

public interface IPipelineCompressor 
{
    public Stream Compress(Stream stream);

    public Stream Decompress(Stream stream);
}
