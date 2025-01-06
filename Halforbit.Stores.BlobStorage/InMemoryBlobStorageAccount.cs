//using Azure;
//using Azure.Storage.Blobs;
using System.Collections.Concurrent;

namespace Halforbit.Stores;

public record InMemoryBlobStorageAccount
{
    public ConcurrentDictionary<string, InMemoryBlobContainer> Containers { get; init; } = [];
}
