using System.Collections.Concurrent;

namespace Halforbit.Stores;

public record InMemoryBlob
{
    public required string BlobName { get; init; }

    public ConcurrentDictionary<string, InMemoryBlobVersion> Versions { get; init; } = [];
}
