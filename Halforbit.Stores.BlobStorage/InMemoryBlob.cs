//using Azure;
//using Azure.Storage.Blobs;
using System.Collections.Concurrent;

namespace Halforbit.Stores;

public record InMemoryBlob
{
    public required string BlobName { get; init; }

    public required ConcurrentDictionary<string, InMemoryBlobVersion> Versions { get; init; }
}
