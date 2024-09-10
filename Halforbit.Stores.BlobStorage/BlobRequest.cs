using Azure.Storage.Blobs;

namespace Halforbit.Stores;

public static class BlobRequest
{
    public static IBlobRequestWithConnectionString ConnectionString(string connectionString)
    {
        return new BlobRequest<None, None>
        {
            _ConnectionString = connectionString
        };
    }
}

record BlobRequest<TKey, TValue> :
    IBlobRequestWithConnectionString,
    IBlobRequestWithContainer,
    IBlockBlobRequest,
    IBlockBlobRequestWithSerialization,
    IBlockBlobRequestWithCompression,
    IBlockBlobRequestWithFixedKey,
    IBlockBlobRequestWithSingleValue<TValue>,
    IBlockBlobRequestWithKeyMap<TKey>,
    IBlockBlobRequestWithKeyMapValue<TKey, TValue>
{
    public string? _ConnectionString { get; init; }

    public string? _ContainerName { get; init; }

    public BlobContainerClient? BlobContainerClient { get; init; }

    public IPipelineSerializer? Serializer { get; init; }

    public IContentSerializer? ContentSerializer { get; init; }

    public string? ContentType { get; init; }

    public string? ContentTypeExtension { get; init; }

    public IPipelineCompressor? Compressor { get; init; }

    public ICompressionStrategy? CompressionStrategy { get; init; }

    public string? ContentEncoding { get; init; }

    public string? ContentEncodingExtension { get; init; }

    public string? Key { get; init; }

    public KeyMap<TKey>? KeyMap { get; init; }

    public BlobRequest<TK, TV> RecastTo<TK, TV>()
    {
        return new BlobRequest<TK, TV>
        {
            _ConnectionString = _ConnectionString,
            _ContainerName = _ContainerName,
            BlobContainerClient = BlobContainerClient,
            Serializer = Serializer,
            ContentSerializer = ContentSerializer,
            ContentType = ContentType,
            ContentTypeExtension = ContentTypeExtension,
            Compressor = Compressor,
            CompressionStrategy = CompressionStrategy,
            ContentEncoding = ContentEncoding,
            ContentEncodingExtension = ContentEncodingExtension,
            Key = Key,
        };
    }

    IBlockBlobRequestWithSingleValue<TValue1> IBlockBlobRequestWithFixedKey.Value<TValue1>()
    {
        return new BlobRequest<None, TValue1>
        {
            _ConnectionString = _ConnectionString,
            _ContainerName = _ContainerName,
            BlobContainerClient = BlobContainerClient,
            Serializer = Serializer,
            ContentSerializer = ContentSerializer,
            ContentType = ContentType,
            ContentTypeExtension = ContentTypeExtension,
            Compressor = Compressor,
            CompressionStrategy = CompressionStrategy,
            ContentEncoding = ContentEncoding,
            ContentEncodingExtension = ContentEncodingExtension,
            Key = Key,
        };
    }

    IBlockBlobRequestWithKeyMapValue<TKey, TValue1> IBlockBlobRequestWithKeyMap<TKey>.Value<TValue1>()
    {
        return new BlobRequest<TKey, TValue1>
        {
            _ConnectionString = _ConnectionString,
            _ContainerName = _ContainerName,
            BlobContainerClient = BlobContainerClient,
            Serializer = Serializer,
            ContentSerializer = ContentSerializer,
            ContentType = ContentType,
            ContentTypeExtension = ContentTypeExtension,
            Compressor = Compressor,
            CompressionStrategy = CompressionStrategy,
            ContentEncoding = ContentEncoding,
            ContentEncodingExtension = ContentEncodingExtension,
            KeyMap = KeyMap
        };
    }
}
