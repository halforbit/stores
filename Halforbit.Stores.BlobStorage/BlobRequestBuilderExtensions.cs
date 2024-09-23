using Azure.Storage.Blobs;
using OpenTelemetry.Trace;
using System.Linq.Expressions;

namespace Halforbit.Stores;

public static class BlobRequestBuilderExtensions
{
    public static IBlobStorageAccount Trace(
        this IBlobStorageAccount request, 
        TracerProvider tracerProvider)
    {
        var q = (BlobRequest<None, None>)request;

        return q with
        {
            Tracer = tracerProvider.GetTracer("Halforbit.Stores")
        };
    }

    public static IBlobStorageAccount HttpClientFactory(
        this IBlobStorageAccount request,
        IHttpClientFactory httpClientFactory)
    {
        var q = (BlobRequest<None, None>)request;

        return q with
        { 
            HttpClientFactory = httpClientFactory
        };
    }

    public static IBlobContainer Container(
        this IBlobStorageAccount request,
        string name)
    {
        var q = (BlobRequest<None, None>)request;

        return q with
        {
            _ContainerName = name,

            BlobContainerClient = new BlobContainerClient(q._ConnectionString, name)
        };
    }

    public static async Task<CreateContainerResponse> CreateContainerIfNotExistsAsync(
        this IBlobContainer request)
    {
        var q = (BlobRequest<None, None>)request;

        if (q.BlobContainerClient is null) throw new Exception("BlobContainerClient has not been created.");

        var response = await q.BlobContainerClient.CreateIfNotExistsAsync();

        return new();
    }

    public static async Task<DeleteContainerResponse> DeleteContainerAsync(
        this IBlobContainer request)
    {
		var q = (BlobRequest<None, None>)request;

		if (q.BlobContainerClient is null) throw new Exception("BlobContainerClient has not been created.");

        await q.BlobContainerClient.DeleteIfExistsAsync();

        return new();
	}

	public static IBlockBlob BlockBlobs(
        this IBlobContainer request) => (BlobRequest<None, None>)request with
        {
            BlobType = BlobType.BlockBlob
        };

    public static ISerializedBlockBlob JsonSerialization(
        this IBlockBlob request) => (BlobRequest<None, None>)request with
        {
            Serializer = new JsonPipelineSerializer(),

            ContentSerializer = new JsonSerializerStrategy(),

            ContentType = "application/json",

            ContentTypeExtension = ".json"
        };

    public static ICompressedBlockBlob GZipCompression(
        this ISerializedBlockBlob request) => (BlobRequest<None, None>)request with
        {
            Compressor = new GZipPipelineCompressor(),

            CompressionStrategy = new GzipCompressionStrategy(),

            ContentEncoding = "gzip",

            ContentEncodingExtension = ".gz"
        };

    public static IBlockBlobs<TKey> Key<TKey>(
        this ISerializedBlockBlob request,
        Expression<Func<TKey, string>> map)
    {
        var q = (BlobRequest<None, None>)request;

        return q.RecastTo<TKey, None>() with
        {
            KeyMap = KeyMap<TKey>.Define(map, $"{q.ContentTypeExtension}{q.ContentEncodingExtension}")
        };
    }

    public static INamedBlockBlob Name(
        this ISerializedBlockBlob request,
        string name) => (BlobRequest<None, None>)request with
        {
            Name = name
        };

    public static INamedBlockBlob Name(
        this ICompressedBlockBlob request,
        string name) => (BlobRequest<None, None>)request with
        {
            Name = name
        };

    public static IBlockBlobs<TKey, TValue> WithMetadata<TKey, TValue>(
        this IBlockBlobs<TKey, TValue> request) => ((BlobRequest<TKey, TValue>)request) with
        {
            IncludeMetadata = true
        };

    public static IBlockBlobs<TKey, TValue> WithoutMetadata<TKey, TValue>(
        this IBlockBlobs<TKey, TValue> request) => ((BlobRequest<TKey, TValue>)request) with
        {
            IncludeMetadata = false
        };

    public static IBlockBlob<TValue> WithMetadata<TValue>(
        this IBlockBlob<TValue> request) => ((BlobRequest<None, TValue>)request) with
        {
            IncludeMetadata = true
        };

    public static IBlockBlob<TValue> WithoutMetadata<TValue>(
        this IBlockBlob<TValue> request) => ((BlobRequest<None, TValue>)request) with
        {
            IncludeMetadata = false
        };
}
