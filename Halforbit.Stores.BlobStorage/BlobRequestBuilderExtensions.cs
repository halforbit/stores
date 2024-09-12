using Azure.Storage.Blobs;
using OpenTelemetry.Trace;
using System.Linq.Expressions;

namespace Halforbit.Stores;

public static class BlobRequestBuilderExtensions
{
    public static IBlobRequestWithConnectionString Trace(
        this IBlobRequestWithConnectionString request, 
        TracerProvider tracerProvider)
    {
        var q = (BlobRequest<None, None>)request;

        return q with
        {
            Tracer = tracerProvider.GetTracer("Halforbit.Stores")
        };
    }

    public static IBlobRequestWithConnectionString HttpClientFactory(
        this IBlobRequestWithConnectionString request,
        IHttpClientFactory httpClientFactory)
    {
        var q = (BlobRequest<None, None>)request;

        return q with
        { 
            HttpClientFactory = httpClientFactory
        };
    }

    public static IBlobRequestWithContainer Container(
        this IBlobRequestWithConnectionString request,
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
        this IBlobRequestWithContainer request)
    {
        var q = (BlobRequest<None, None>)request;

        if (q.BlobContainerClient is null) throw new Exception("BlobContainerClient has not been created.");

        var response = await q.BlobContainerClient.CreateIfNotExistsAsync();

        return new();
    }

    public static async Task<DeleteContainerResponse> DeleteContainerAsync(
        this IBlobRequestWithContainer request)
    {
		var q = (BlobRequest<None, None>)request;

		if (q.BlobContainerClient is null) throw new Exception("BlobContainerClient has not been created.");

        await q.BlobContainerClient.DeleteIfExistsAsync();

        return new();
	}

	public static IBlockBlobRequest BlockBlobs(
        this IBlobRequestWithContainer request) => (BlobRequest<None, None>)request with
        {
            BlobType = BlobType.BlockBlob
        };

    public static IBlockBlobRequestWithSerialization JsonSerialization(
        this IBlockBlobRequest request) => (BlobRequest<None, None>)request with
        {
            Serializer = new JsonPipelineSerializer(),

            ContentSerializer = new JsonSerializerStrategy(),

            ContentType = "application/json",

            ContentTypeExtension = ".json"
        };

    public static IBlockBlobRequestWithCompression GZipCompression(
        this IBlockBlobRequestWithSerialization request) => (BlobRequest<None, None>)request with
        {
            Compressor = new GZipPipelineCompressor(),

            CompressionStrategy = new GzipCompressionStrategy(),

            ContentEncoding = "gzip",

            ContentEncodingExtension = ".gz"
        };

    public static IBlockBlobRequestWithKeyMap<TKey> Key<TKey>(
        this IBlockBlobRequestWithSerialization request,
        Expression<Func<TKey, string>> map)
    {
        var q = (BlobRequest<None, None>)request;

        return q.RecastTo<TKey, None>() with
        {
            KeyMap = KeyMap<TKey>.Define(map, $"{q.ContentTypeExtension}{q.ContentEncodingExtension}")
        };
    }
    public static IBlockBlobRequestWithFixedKey Key(
        this IBlockBlobRequestWithSerialization request,
        string key) => (BlobRequest<None, None>)request with
        {
            Key = key
        };

    public static IBlockBlobRequestWithFixedKey Key(
        this IBlockBlobRequestWithCompression request,
        string key) => (BlobRequest<None, None>)request with
        {
            Key = key
        };
}
