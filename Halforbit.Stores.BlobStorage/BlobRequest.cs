﻿using Azure.Storage.Blobs;
using OpenTelemetry.Trace;

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
    IBlockBlob,
    ISerializedBlockBlob,
    ICompressedBlockBlob,
    INamedBlockBlob,
    IBlockBlob<TValue>,
    IBlockBlobs<TKey>,
    IBlockBlobs<TKey, TValue>
{
    public Tracer? Tracer { get; init; }

    public IHttpClientFactory? HttpClientFactory { get; init; }

    public string? _ConnectionString { get; init; }

    public string? _ContainerName { get; init; }

    public BlobContainerClient? BlobContainerClient { get; init; }

    public BlobType BlobType { get; init; }

    public IPipelineSerializer? Serializer { get; init; }

    public IContentSerializer? ContentSerializer { get; init; }

    public string? ContentType { get; init; }

    public string? ContentTypeExtension { get; init; }

    public IPipelineCompressor? Compressor { get; init; }

    public ICompressionStrategy? CompressionStrategy { get; init; }

    public string? ContentEncoding { get; init; }

    public string? ContentEncodingExtension { get; init; }

    public bool IncludeMetadata { get; init; }

    public string? Name { get; init; }

    public KeyMap<TKey>? KeyMap { get; init; }

    public BlobRequest<TK, TV> RecastTo<TK, TV>()
    {
        return new BlobRequest<TK, TV>
        {
            Tracer = Tracer,
            HttpClientFactory = HttpClientFactory,
            _ConnectionString = _ConnectionString,
            _ContainerName = _ContainerName,
            BlobContainerClient = BlobContainerClient,
            BlobType = BlobType,
            Serializer = Serializer,
            ContentSerializer = ContentSerializer,
            ContentType = ContentType,
            ContentTypeExtension = ContentTypeExtension,
            Compressor = Compressor,
            CompressionStrategy = CompressionStrategy,
            ContentEncoding = ContentEncoding,
            ContentEncodingExtension = ContentEncodingExtension,
            IncludeMetadata = IncludeMetadata,
            Name = Name,
        };
    }

    IBlockBlob<TValue1> INamedBlockBlob.Value<TValue1>()
    {
        return new BlobRequest<None, TValue1>
        {
            Tracer = Tracer,
			HttpClientFactory = HttpClientFactory,
			_ConnectionString = _ConnectionString,
            _ContainerName = _ContainerName,
            BlobContainerClient = BlobContainerClient,
            BlobType = BlobType,
            Serializer = Serializer,
            ContentSerializer = ContentSerializer,
            ContentType = ContentType,
            ContentTypeExtension = ContentTypeExtension,
            Compressor = Compressor,
            CompressionStrategy = CompressionStrategy,
            ContentEncoding = ContentEncoding,
            ContentEncodingExtension = ContentEncodingExtension,
            IncludeMetadata = IncludeMetadata,
            Name = Name,
        };
    }

    IBlockBlobs<TKey, TValue1> IBlockBlobs<TKey>.Value<TValue1>()
    {
        return new BlobRequest<TKey, TValue1>
        {
            Tracer = Tracer,
			HttpClientFactory = HttpClientFactory,
			_ConnectionString = _ConnectionString,
            _ContainerName = _ContainerName,
            BlobContainerClient = BlobContainerClient,
            BlobType = BlobType,
            Serializer = Serializer,
            ContentSerializer = ContentSerializer,
            ContentType = ContentType,
            ContentTypeExtension = ContentTypeExtension,
            Compressor = Compressor,
            CompressionStrategy = CompressionStrategy,
            ContentEncoding = ContentEncoding,
            ContentEncodingExtension = ContentEncodingExtension,
            IncludeMetadata = IncludeMetadata,
            KeyMap = KeyMap
        };
    }
}
