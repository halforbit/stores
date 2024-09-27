using Azure;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.IO;

namespace Halforbit.Stores;

public static class BlobRequestOperationExtensions
{
    static RecyclableMemoryStreamManager _recyclableMemoryStreamManager = 
        new RecyclableMemoryStreamManager();

    static RecyclableDictionaryManager<string, string> _recyclableDictionaryManager = 
        new RecyclableDictionaryManager<string, string>();

    public static IAsyncEnumerable<Blob> EnumerateBlobsAsync(
        this IBlobContainer request,
        string? prefix = null)
    {
        var q = (BlobRequest<None, None>)request;

        return EnumerateBlobsAsync(q, prefix);
    }

    public static IAsyncEnumerable<Blob> EnumerateBlobsAsync<TKey, TValue>(
        this IBlockBlobs<TKey, TValue> request,
        object? partialKey = null)
    {
        var q = (BlobRequest<TKey, TValue>)request;

        if (q.KeyMap is null) throw new ArgumentNullException(nameof(q.KeyMap));

        if (!q.KeyMap.TryMapPartialKeyToPrefixString(partialKey, out var prefix))
        {
            throw new ArgumentException("Failed to map partial key to prefix string.");
        }

        return EnumerateBlobsAsync(q, prefix);
    }
	
    static async IAsyncEnumerable<Blob> EnumerateBlobsAsync<TKey, TValue>(
		BlobRequest<TKey, TValue> q,
        string? prefix = null)
    {
		if (q.BlobContainerClient is null) throw new ArgumentNullException(nameof(q.BlobContainerClient));
        
        await foreach (var blobItem in q.BlobContainerClient.GetBlobsAsync(
            q.IncludeMetadata ? BlobTraits.Metadata : default,
            q.IncludeVersions ? BlobStates.Version : default,
            prefix))
        {
            if (q.KeyMap is not null && 
                !q.KeyMap.TryMapStringToKey(blobItem.Name, out var key))
            {
                continue;
            }

            if (q.BlobType != BlobType.Unknown)
            {
                if (q.BlobType != ConvertBlobType(blobItem.Properties.BlobType))
                {
                    continue;
                }
            }

            yield return new()
            {
                Name = blobItem.Name,
                
                ETag = blobItem.Properties.ETag?.ToString() ?? throw new ArgumentNullException("ETag"),
                
                VersionId = blobItem.VersionId,
                
                Metadata = blobItem.Metadata ?? new Dictionary<string, string>(),
                
                CreationTime = blobItem.Properties.CreatedOn?.UtcDateTime ?? throw new ArgumentNullException("CreatedOn"),
                
                LastModified = blobItem.Properties.LastModified?.UtcDateTime ?? throw new ArgumentNullException("LastModified"),
                
                BlobType = ConvertBlobType(blobItem.Properties.BlobType ?? throw new ArgumentNullException("BlobType")),
                
                ContentLength = blobItem.Properties.ContentLength ?? throw new ArgumentNullException("ContentLength"),
                
                ContentType = blobItem.Properties.ContentType ?? throw new ArgumentNullException("ContentType"),
                
                ContentEncoding = blobItem.Properties.ContentEncoding,

                ContentHash = blobItem.Properties.ContentHash ?? throw new ArgumentNullException("ContentHash")
            };
        }
    }

    static BlobType ConvertBlobType(Azure.Storage.Blobs.Models.BlobType? blobType)
    {
        switch (blobType)
        {
            case null: return BlobType.Unknown;
            
            case Azure.Storage.Blobs.Models.BlobType.Block: return BlobType.BlockBlob;
            
            case Azure.Storage.Blobs.Models.BlobType.Append: return BlobType.AppendBlob;
            
            case Azure.Storage.Blobs.Models.BlobType.Page: return BlobType.PageBlob;
            
            default: throw new ArgumentException("Unhandled blob type.");
        }
    }

    public static Task<PutResult> UpsertBlobAsync<TValue>(
        this IBlockBlob<TValue> request,
        TValue value,
        IDictionary<string, string>? metadata = null)
    {
		var q = (BlobRequest<None, TValue>)request;

        return UpsertBlobAsync(
            q: q,
            key: None.Instance, 
            value: value, 
            metadata: metadata);
    }

	public static Task<PutResult> UpsertBlobAsync<TKey, TValue>(
		this IBlockBlobs<TKey, TValue> request,
		TKey key, 
        TValue value,
		IDictionary<string, string>? metadata = null)
    {
		var q = (BlobRequest<TKey, TValue>)request;

		return UpsertBlobAsync(
			q: q,
			key: key,
			value: value,
			metadata: metadata);
	}

    static async Task<PutResult> UpsertBlobAsync<TKey, TValue>(
        BlobRequest<TKey, TValue> q,
        TKey key,
        TValue value,
        IDictionary<string, string>? metadata = null)
    {        
        if (q.ContentSerializer is null) throw new ArgumentNullException("Serializer is not specified.");
        
        using var span = q.Tracer?.StartActiveSpan(nameof(UpsertBlobAsync));

        var blobName =
            key is None ? 
            BuildBlobName(q) :
            BuildBlobName(q, key);

		span?.SetAttribute("BlobName", blobName);

		using var ms = _recyclableMemoryStreamManager.GetStream();

		using (var _ = q.Tracer?.StartActiveSpan("Serialize"))
		{
			var pipeline = new ContentPipeline(q.ContentSerializer, q.CompressionStrategy);

			await pipeline.SerializeAndWriteAsync(value, ms);
		}

		span?.SetAttribute("BlobLength", ms.Length);

		ms.Seek(0, SeekOrigin.Begin);

        BlobContentInfo blobInfo;

        var blobHttpHeaders = new BlobHttpHeaders
        {
            ContentType = q.ContentType,
            ContentEncoding = q.ContentEncoding
        };

        using (var _ = q.Tracer?.StartActiveSpan("Transmit"))
        {
            try
            {
                blobInfo = await q.BlobContainerClient
                    .GetBlockBlobClient(blobName)
                    .UploadAsync(
                        content: ms, 
                        options: new()
                        {
                            HttpHeaders = blobHttpHeaders,
                            Metadata = metadata, 
                            Conditions = q.IfMatch is not null ? 
                                new BlobRequestConditions
                                {
                                    IfMatch = new ETag(q.IfMatch), 
                                } : 
                                null
                        });
            }
            catch (RequestFailedException rfex)
            {
                throw new ConditionFailedException();
            }
        }

        var eTag = blobInfo.ETag.ToString();

        span?.SetAttribute("ETag", eTag);

        span?.SetAttribute("VersionId", blobInfo.VersionId);

        return new PutResult
        {
            ETag = eTag,
            Name = blobName,
            VersionId = blobInfo.VersionId
        };
	}

    public static async Task<bool> BlobExistsAsync<TValue>(
        this IBlockBlob<TValue> request)
    {
        var q = (BlobRequest<None, TValue>)request;

        if (q.BlobContainerClient is null) throw new Exception("BlobContainerClient is not initialized.");

        return await q.BlobContainerClient
            .GetBlobClient(BuildBlobName(q))
            .ExistsAsync();
    }

	public static async Task<bool> BlobExistsAsync<TKey, TValue>(
		this IBlockBlobs<TKey, TValue> request,
        TKey key)
	{
		var q = (BlobRequest<TKey, TValue>)request;

		if (q.BlobContainerClient is null) throw new ArgumentNullException(nameof(q.BlobContainerClient));

		return await q.BlobContainerClient
			.GetBlobClient(BuildBlobName(q, key))
			.ExistsAsync();
	}

	public static async Task<bool> DeleteBlobAsync<TValue>(
        this IBlockBlob<TValue> request)
    {
        var q = (BlobRequest<None, TValue>)request;

        if (q.BlobContainerClient is null) throw new Exception("BlobContainerClient is not initialized.");

        return await q.BlobContainerClient
            .GetBlobClient(BuildBlobName(q))
            .DeleteIfExistsAsync();
    }

	public static async Task<bool> DeleteBlobAsync<TKey, TValue>(
		this IBlockBlobs<TKey, TValue> request,
        TKey key)
	{
		var q = (BlobRequest<TKey, TValue>)request;

		if (q.BlobContainerClient is null) throw new Exception("BlobContainerClient is not initialized.");

		return await q.BlobContainerClient
			.GetBlobClient(BuildBlobName(q, key))
			.DeleteIfExistsAsync();
	}

	public static Task<Blob<TValue>?> GetBlobOrNullAsync<TValue>(
		this IBlockBlob<TValue> request)
    {
        var q = (BlobRequest<None, TValue>)request;

        return GetBlobOrNullAsync(q);
    }

	public static Task<Blob<TValue>?> GetBlobOrNullAsync<TKey, TValue>(
		this IBlockBlobs<TKey, TValue> request,
        TKey key)
	{
		var q = (BlobRequest<TKey, TValue>)request;

		return GetBlobOrNullAsync(q, key);
	}

    static async Task<Blob<TValue>?> GetBlobOrNullAsync<TKey, TValue>(
        BlobRequest<TKey, TValue> q,
        TKey? key = default)
    {
        if (q.BlobContainerClient is null) throw new Exception("BlobContainerClient is not initialized.");

        if (q.ContentSerializer is null) throw new Exception("Serializer is not specified.");

        using var span = q.Tracer?.StartActiveSpan(nameof(GetBlobOrNullAsync));

        var blobName = key is null ? 
            BuildBlobName(q) : 
            BuildBlobName(q, key);

        span?.SetAttribute("BlobName", blobName);

        var blobClient = q.BlobContainerClient.GetBlockBlobClient(blobName);

        if (q.VersionId is not null)
        {
            blobClient = blobClient.WithVersion(q.VersionId);

            span?.SetAttribute("VersionId", q.VersionId);
        }

        Response<BlobDownloadInfo> response;

        try
        {
            response = await blobClient.DownloadAsync();
        }
        catch (RequestFailedException rfex) when (rfex.Status == 404)
        {
            return null;
        }

        var pipeline = new ContentPipeline(
            q.ContentSerializer, 
            q.CompressionStrategy);

        var value = await pipeline.ReadAndDeserializeAsync<TValue>(response.Value.Content);

        if (value is null)
        {
            return null;
        }

        var headers = response.GetRawResponse().Headers;

        var metadata = default(IDictionary<string, string>?);

        if (q.IncludeMetadata)
        {
            metadata = _recyclableDictionaryManager.Get();

            foreach (var header in headers)
            {
                if (header.Name.StartsWith("x-ms-meta-"))
                {
                    metadata[header.Name["x-ms-meta-".Length..]] = header.Value;
                }
            }
        }

        return new()
        {
            Name = blobName,
            
            Value = value,
            
            ETag = headers.ETag?.ToString() ?? string.Empty,
            
            VersionId = headers.TryGetValue("x-ms-version-id", out var version) ? version : null,
            
            Metadata = metadata,
            
            CreationTime = headers.TryGetValue("x-ms-creation-time", out var creationTime) ?
                DateTime.TryParse(creationTime, out var ct) ?
                    ct :
                    throw new ArgumentException("CreationTime") :
                throw new ArgumentException("CreationTime"),
            
            LastModified = headers.TryGetValue("Last-Modified", out var lastModified) ?
                DateTime.TryParse(lastModified, out var lm) ?
                    lm :
                    throw new ArgumentException("LastModified") :
                throw new ArgumentException("LastModified"),
            
            BlobType = headers.TryGetValue("x-ms-blob-type", out var blobType) ?
                Enum.Parse<BlobType>(blobType) :
                BlobType.Unknown,
            
            ContentLength = headers.TryGetValue("Content-Length", out var contentLength) ? 
                long.TryParse(contentLength, out var cl) ? 
                    cl : 
                    throw new ArgumentException("ContentLength") : 
                throw new ArgumentException("ContentLength"),
            
            ContentType = headers.TryGetValue("Content-Type", out var contentType) ?
                contentType :
                throw new ArgumentException("ContentType"),
            
            ContentEncoding = headers.TryGetValue("Content-Encoding", out var contentEncoding) ? 
                contentEncoding : 
                null,
            
            ContentHash = headers.TryGetValue("Content-MD5", out var contentMd5) ? 
                Convert.FromBase64String(contentMd5) : 
                Array.Empty<byte>()
        };
    }

    static string BuildBlobName<TKey, TValue>(
        BlobRequest<TKey, TValue> q)
    {
        return $"{q.Name}{q.ContentTypeExtension}{q.ContentEncodingExtension}";
    }

    static string BuildBlobName<TKey, TValue>(
        BlobRequest<TKey, TValue> q,
        TKey key)
    {
        if (q.KeyMap is null) throw new ArgumentNullException(nameof(q.KeyMap));
    
        if (!q.KeyMap.TryMapKeyToString(key, out var name))
        {
            throw new ArgumentException("Could not map key to string.");
        }

        return name;    
    }
}
