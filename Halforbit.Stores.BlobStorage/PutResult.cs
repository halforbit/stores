namespace Halforbit.Stores;

public record PutResult
{
    public required string Name { get; init; }

    public required string ETag { get; init; }

    public required DateTime LastModified { get; init; }

    public required string? VersionId { get; init; }
}
