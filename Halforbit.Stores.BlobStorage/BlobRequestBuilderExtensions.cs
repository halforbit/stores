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

    public static IBlobContainer Container(
        this IBlobStorageAccount request,
        string name)
    {
        var q = (BlobRequest<None, None>)request;

        return q with
        {
            _ContainerName = name,

            BlobContainerClient = q._ConnectionString is not null ?
                new AzureBlobContainerClient(
                    q._ConnectionString,
                    name) :
                q.InMemoryBlobStorageAccount is not null ?
                    new InProcessBlobContainerClient(q.InMemoryBlobStorageAccount, name) :
                    throw new ArgumentException("Connection string not provided.")
        };
    }

    public static async Task CreateContainerIfNotExistsAsync(
        this IBlobContainer request)
    {
        var q = (BlobRequest<None, None>)request;

        if (q.BlobContainerClient is null) throw new Exception("BlobContainerClient has not been created.");

        /*var response = */await q.BlobContainerClient.CreateIfNotExistsAsync();
    }

    public static async Task DeleteContainerAsync(
        this IBlobContainer request)
    {
		var q = (BlobRequest<None, None>)request;

		if (q.BlobContainerClient is null) throw new Exception("BlobContainerClient has not been created.");

        /*var response = */await q.BlobContainerClient.DeleteIfExistsAsync();
	}

	public static IBlockBlobs BlockBlobs(
        this IBlobContainer request) => (BlobRequest<None, None>)request with
        {
            BlobType = BlobType.BlockBlob
        };

    public static ISerializedBlockBlobs JsonSerialization(
        this IBlockBlobs request) => (BlobRequest<None, None>)request with
        {
            ContentSerializer = new JsonSerializerStrategy(),

            ContentType = "application/json",

            ContentTypeExtension = ".json"
        };

    public static ISerializedBlockBlobs NoSerialization(
        this IBlockBlobs request) => (BlobRequest<None, None>)request with
        {
            ContentSerializer = new NoSerializerStrategy(),
        };

    public static ICompressedBlockBlobs GZipCompression(
        this ISerializedBlockBlobs request) => (BlobRequest<None, None>)request with
        {
            CompressionStrategy = new GZipCompressionStrategy(),

            ContentEncoding = "gzip",

            ContentEncodingExtension = ".gz"
        };

    public static IEmptyBlockBlobs Empty(
        this IBlockBlobs request)
    {
        return (BlobRequest<None, None>)request;
    }

    public static IBlockBlobs<TKey> Key<TKey>(
        this ISerializedBlockBlobs request,
        Expression<Func<TKey, string>> map)
    {
        var q = (BlobRequest<None, None>)request;

        return q.RecastTo<TKey, None>() with
        {
            KeyMap = KeyMap<TKey>.Define(map, $"{q.ContentTypeExtension}{q.ContentEncodingExtension}")
        };
    }

    public static IEmptyBlockBlobs<TKey> Key<TKey>(
        this IEmptyBlockBlobs request,
        Expression<Func<TKey, string>> map)
    {
        var q = (BlobRequest<None, None>)request;

        return q.RecastTo<TKey, None>() with
        {
            KeyMap = KeyMap<TKey>.Define(map, $"{q.ContentTypeExtension}{q.ContentEncodingExtension}")
        };
    }

    public static IBlockBlob Name(
        this ISerializedBlockBlobs request,
        string name) => (BlobRequest<None, None>)request with
        {
            Name = name
        };

    public static IBlockBlob Name(
        this ICompressedBlockBlobs request,
        string name) => (BlobRequest<None, None>)request with
        {
            Name = name
        };

    public static IEmptyBlockBlob Name(
        this IEmptyBlockBlobs request,
        string name) => (BlobRequest<None, None>)request with
        {
            Name = name
        };

    public static IBlobContainer WithMetadata(
        this IBlobContainer request) => ((BlobRequest<None, None>)request) with
        {
            IncludeMetadata = true
        };

    public static IBlobContainer WithoutMetadata(
        this IBlobContainer request) => ((BlobRequest<None, None>)request) with
        {
            IncludeMetadata = false
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

    public static IEmptyBlockBlobs<TKey> WithMetadata<TKey>(
        this IEmptyBlockBlobs<TKey> request) => ((BlobRequest<TKey, None>)request) with
        {
            IncludeMetadata = true
        };

    public static IEmptyBlockBlobs<TKey> WithoutMetadata<TKey>(
        this IEmptyBlockBlobs<TKey> request) => ((BlobRequest<TKey, None>)request) with
        {
            IncludeMetadata = false
        };

    public static IEmptyBlockBlob WithMetadata(
        this IEmptyBlockBlob request) => ((BlobRequest<None, None>)request) with
        {
            IncludeMetadata = true
        };

    public static IEmptyBlockBlob WithoutMetadata(
        this IEmptyBlockBlob request) => ((BlobRequest<None, None>)request) with
        {
            IncludeMetadata = false
        };

    public static IBlobContainer WithVersions(
        this IBlobContainer request) => ((BlobRequest<None, None>)request) with
        {
            IncludeVersions = true
        };

    public static IBlobContainer WithoutVersions(
        this IBlobContainer request) => ((BlobRequest<None, None>)request) with
        {
            IncludeVersions = false
        };

    public static IBlockBlobs<TKey, TValue> WithVersions<TKey, TValue>(
        this IBlockBlobs<TKey, TValue> request) => ((BlobRequest<TKey, TValue>)request) with
        {
            IncludeVersions = true
        };

    public static IBlockBlobs<TKey, TValue> WithoutVersions<TKey, TValue>(
        this IBlockBlobs<TKey, TValue> request) => ((BlobRequest<TKey, TValue>)request) with
        {
            IncludeVersions = false
        };

    public static IEmptyBlockBlobs<TKey> WithVersions<TKey>(
        this IEmptyBlockBlobs<TKey> request) => ((BlobRequest<TKey, None>)request) with
        {
            IncludeVersions = true
        };

    public static IEmptyBlockBlobs<TKey> WithoutVersions<TKey>(
        this IBlockBlobs<TKey> request) => ((BlobRequest<TKey, None>)request) with
        {
            IncludeVersions = false
        };

    public static IEmptyBlockBlob WithVersions(
        this IEmptyBlockBlob request) => ((BlobRequest<None, None>)request) with
        {
            IncludeVersions = true
        };

    public static IEmptyBlockBlob WithoutVersions(
        this IBlockBlob request) => ((BlobRequest<None, None>)request) with
        {
            IncludeVersions = false
        };

    public static IBlobContainer Version(
        this IBlobContainer request,
        string versionId) => ((BlobRequest<None, None>)request) with
        {
            VersionId = versionId
        };

    public static IBlockBlobs<TKey, TValue> Version<TKey, TValue>(
        this IBlockBlobs<TKey, TValue> request,
        string versionId) => ((BlobRequest<TKey, TValue>)request) with
        {
            VersionId = versionId
        };

    public static IBlockBlob<TValue> Version<TValue>(
        this IBlockBlob<TValue> request,
        string versionId) => ((BlobRequest<None, TValue>)request) with
        {
            VersionId = versionId
        };

    public static IEmptyBlockBlob Version(
        this IEmptyBlockBlob request,
        string versionId) => ((BlobRequest<None, None>)request) with
        {
            VersionId = versionId
        };

    public static IEmptyBlockBlobs<TKey> Version<TKey>(
        this IEmptyBlockBlobs<TKey> request,
        string versionId) => ((BlobRequest<TKey, None>)request) with
        {
            VersionId = versionId
        };

    public static IBlockBlob<TValue> IfMatch<TValue>(
        this IBlockBlob<TValue> request,
        string eTag) => ((BlobRequest<None, TValue>)request) with
        {
            IfMatch = eTag
        };

    public static IBlockBlobs<TKey, TValue> IfMatch<TKey, TValue>(
        this IBlockBlobs<TKey, TValue> request,
        string eTag) => ((BlobRequest<TKey, TValue>)request) with
        {
            IfMatch = eTag
        };

    public static IBlockBlob<TValue> IfExists<TValue>(
        this IBlockBlob<TValue> request) => ((BlobRequest<None, TValue>)request) with
        {
            IfExists = true
        };

    public static IBlockBlobs<TKey, TValue> IfExists<TKey, TValue>(
        this IBlockBlobs<TKey, TValue> request) => ((BlobRequest<TKey, TValue>)request) with
        {
            IfExists = true
        };

    public static IBlockBlob<TValue> IfNotExists<TValue>(
        this IBlockBlob<TValue> request) => ((BlobRequest<None, TValue>)request) with
        {
            IfNotExists = true
        };

    public static IBlockBlobs<TKey, TValue> IfNotExists<TKey, TValue>(
        this IBlockBlobs<TKey, TValue> request) => ((BlobRequest<TKey, TValue>)request) with
        {
            IfNotExists = true
        };

    public static IBlockBlob<TValue> IfModifiedSince<TValue>(
        this IBlockBlob<TValue> request,
        DateTime modifiedSinceTime) => ((BlobRequest<None, TValue>)request) with
        {
            IfModifiedSince = modifiedSinceTime
        };

    public static IBlockBlobs<TKey, TValue> IfModifiedSince<TKey, TValue>(
        this IBlockBlobs<TKey, TValue> request,
        DateTime modifiedSinceTime) => ((BlobRequest<TKey, TValue>)request) with
        {
            IfModifiedSince = modifiedSinceTime
        };

    public static IBlockBlob<TValue> IfUnmodifiedSince<TValue>(
        this IBlockBlob<TValue> request,
        DateTime unmodifiedSinceTime) => ((BlobRequest<None, TValue>)request) with
        {
            IfUnmodifiedSince = unmodifiedSinceTime
        };

    public static IBlockBlobs<TKey, TValue> IfUnmodifiedSince<TKey, TValue>(
        this IBlockBlobs<TKey, TValue> request,
        DateTime unmodifiedSinceTime) => ((BlobRequest<TKey, TValue>)request) with
        {
            IfUnmodifiedSince = unmodifiedSinceTime
        };

    public static ISerializedBlockBlobs FileExtension(
        this ISerializedBlockBlobs request,
        string fileExtension) => ((BlobRequest<None, None>)request) with
        {
            ContentEncodingExtension = string.Empty,
            ContentTypeExtension = fileExtension
        };

    public static ISerializedBlockBlobs NoFileExtension(
        this ISerializedBlockBlobs request) => ((BlobRequest<None, None>)request) with
        {
            ContentEncodingExtension = string.Empty,
            ContentTypeExtension = string.Empty
        };
}
