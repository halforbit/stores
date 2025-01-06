//using Azure;
//using Azure.Storage.Blobs;
namespace Halforbit.Stores;

public record InMemoryBlobVersion
{
    public required Blob Blob { get; init; }

    public required ReadOnlyMemory<byte> Content { get; init; }
}
