namespace Halforbit.Stores;

public record Blob
{
    public required string Name { get; init; }

    public required string ETag { get; init; }

    public required string? VersionId { get; init; }

    public required IDictionary<string, string>? Metadata { get; init; }

    public required DateTime CreationTime { get; init; }

    public required DateTime LastModified { get; init; }

    public required BlobType BlobType { get; init; }

    public required long ContentLength { get; init; }

    public required string ContentType { get; init; }

    public required string? ContentEncoding { get; init; }

    public required byte[] ContentHash { get; init; }
}

public record Blob<TValue> : Blob
{
	public required TValue Value { get; init; }
}

