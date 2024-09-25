namespace Halforbit.Stores;

public interface ICompressionStrategy
{
    Stream Compress(Stream output);
    Stream Decompress(Stream input);
}
