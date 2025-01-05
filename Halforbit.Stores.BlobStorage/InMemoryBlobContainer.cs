//using Azure;
//using Azure.Storage.Blobs;
using System.Collections.Concurrent;

namespace Halforbit.Stores;

public record InMemoryBlobContainer
{
    public required string ContainerName { get; init; }

    public required ConcurrentDictionary<string, InMemoryBlob> Blobs { get; init; }
}
