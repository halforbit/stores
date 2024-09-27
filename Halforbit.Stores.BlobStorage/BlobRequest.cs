using Azure.Storage.Blobs;
using OpenTelemetry.Trace;
using System.Security;

namespace Halforbit.Stores;

public static class BlobRequest
{
    public static IBlobStorageAccount ConnectionString(string connectionString)
    {
        return new BlobRequest<None, None>
        {
            _ConnectionString = connectionString
        };
    }
}

record BlobRequest<TKey, TValue> :
    IBlobStorageAccount,
    IBlobContainer,
    IBlockBlobs,
    ISerializedBlockBlobs,
    ICompressedBlockBlobs,
    IBlockBlob,
    IBlockBlob<TValue>,
    IBlockBlobs<TKey>,
    IBlockBlobs<TKey, TValue>
{
    public Tracer? Tracer { get; init; }

    public string? _ConnectionString { get; init; }

    public string? _ContainerName { get; init; }

    public BlobContainerClient? BlobContainerClient { get; init; }

    public BlobType BlobType { get; init; }

    public IContentSerializer? ContentSerializer { get; init; }

    public string? ContentType { get; init; }

    public string? ContentTypeExtension { get; init; }

    public ICompressionStrategy? CompressionStrategy { get; init; }

    public string? ContentEncoding { get; init; }

    public string? ContentEncodingExtension { get; init; }

    public bool IncludeMetadata { get; init; }

    public bool IncludeVersions { get; init; }

    public string? Name { get; init; }

    public string? VersionId { get; init; }

    public string? IfMatch { get; init; }

    public KeyMap<TKey>? KeyMap { get; init; }

    public BlobRequest<TK, TV> RecastTo<TK, TV>()
    {
        return new BlobRequest<TK, TV>
        {
            Tracer = Tracer,
            _ConnectionString = _ConnectionString,
            _ContainerName = _ContainerName,
            BlobContainerClient = BlobContainerClient,
            BlobType = BlobType,
            ContentSerializer = ContentSerializer,
            ContentType = ContentType,
            ContentTypeExtension = ContentTypeExtension,
            CompressionStrategy = CompressionStrategy,
            ContentEncoding = ContentEncoding,
            ContentEncodingExtension = ContentEncodingExtension,
            IncludeMetadata = IncludeMetadata,
            IncludeVersions = IncludeVersions,
            Name = Name,
            VersionId = VersionId,
            IfMatch = IfMatch,
        };
    }

    IBlockBlob<TValue1> IBlockBlob.Value<TValue1>()
    {
        return new BlobRequest<None, TValue1>
        {
            Tracer = Tracer,
			_ConnectionString = _ConnectionString,
            _ContainerName = _ContainerName,
            BlobContainerClient = BlobContainerClient,
            BlobType = BlobType,
            ContentSerializer = ContentSerializer,
            ContentType = ContentType,
            ContentTypeExtension = ContentTypeExtension,
            CompressionStrategy = CompressionStrategy,
            ContentEncoding = ContentEncoding,
            ContentEncodingExtension = ContentEncodingExtension,
            IncludeMetadata = IncludeMetadata,
            IncludeVersions = IncludeVersions,
            Name = Name,
            VersionId = VersionId,
            IfMatch = IfMatch,
        };
    }

    IBlockBlobs<TKey, TValue1> IBlockBlobs<TKey>.Value<TValue1>()
    {
        return new BlobRequest<TKey, TValue1>
        {
            Tracer = Tracer,
			_ConnectionString = _ConnectionString,
            _ContainerName = _ContainerName,
            BlobContainerClient = BlobContainerClient,
            BlobType = BlobType,
            ContentSerializer = ContentSerializer,
            ContentType = ContentType,
            ContentTypeExtension = ContentTypeExtension,
            CompressionStrategy = CompressionStrategy,
            ContentEncoding = ContentEncoding,
            ContentEncodingExtension = ContentEncodingExtension,
            IncludeMetadata = IncludeMetadata,
            IncludeVersions = IncludeVersions,
            Name = Name,
            VersionId = VersionId,
            IfMatch = IfMatch,
            KeyMap = KeyMap
        };
    }
}
