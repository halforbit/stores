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

    public static IAsyncEnumerable<Blob> EnumerateBlobsAsync<TKey>(
        this IEmptyBlockBlobs<TKey> request,
        object? partialKey = null)
    {
        var q = (BlobRequest<TKey, None>)request;

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

            if (q.IfModifiedSince is not null &&
                blobItem.Properties.LastModified > q.IfModifiedSince)
            {
                continue;
            }

            if (q.IfUnmodifiedSince is not null &&
                blobItem.Properties.LastModified <= q.IfUnmodifiedSince)
            {
                continue;
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

    public static Task<PutResult> UpsertBlobAsync(
        this IEmptyBlockBlob request,
        IDictionary<string, string>? metadata = null)
    {
        var q = (BlobRequest<None, None>)request;

        return UpsertBlobAsync(
            q: q,
            key: None.Instance,
            value: None.Instance,
            metadata: metadata);
    }

    public static Task<PutResult> UpsertBlobAsync<TKey>(
        this IEmptyBlockBlobs<TKey> request,
        TKey key,
        IDictionary<string, string>? metadata = null)
    {
        var q = (BlobRequest<TKey, None>)request;

        return UpsertBlobAsync(
            q: q,
            key: key,
            value: None.Instance,
            metadata: metadata);
    }

    static async Task<PutResult> UpsertBlobAsync<TKey, TValue>(
        BlobRequest<TKey, TValue> q,
        TKey key,
        TValue value,
        IDictionary<string, string>? metadata = null)
    {
        if (q.BlobContainerClient is null) throw new Exception("BlobContainerClient is not initialized.");

        var empty = typeof(TValue) == typeof(None);
                
        using var span = q.Tracer?.StartActiveSpan(nameof(UpsertBlobAsync));

        var blobName =
            key is None ? 
            BuildBlobName(q) :
            BuildBlobName(q, key);

		span?.SetAttribute("BlobName", blobName);

		using var ms = _recyclableMemoryStreamManager.GetStream();

        if (!empty)
        {
            if (q.ContentSerializer is null) throw new ArgumentNullException("Serializer is not specified.");

		    using (var _ = q.Tracer?.StartActiveSpan("Serialize"))
		    {
			    var pipeline = new ContentPipeline(
                    q.ContentSerializer, 
                    q.CompressionStrategy);

			    await pipeline.SerializeAndWriteAsync(value, ms);
		    }
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
            var conditions = GetBlobRequestConditions(q);

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
                            Conditions = conditions
                        });
            }
            catch (RequestFailedException rfex) when (rfex.Status == 409)
            {
                throw new PreconditionFailedException();
            }
            catch (RequestFailedException rfex) when (rfex.Status == 412)
            {
                throw new PreconditionFailedException();
            }
        }
        
        var eTag = blobInfo.ETag.ToString();

        span?.SetAttribute("ETag", eTag);

        span?.SetAttribute("VersionId", blobInfo.VersionId);

        return new PutResult
        {
            ETag = eTag,
            Name = blobName,
            LastModified = blobInfo.LastModified.UtcDateTime,
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

    public static async Task<bool> BlobExistsAsync(
        this IEmptyBlockBlob request)
    {
        var q = (BlobRequest<None, None>)request;

        if (q.BlobContainerClient is null) throw new Exception("BlobContainerClient is not initialized.");

        return await q.BlobContainerClient
            .GetBlobClient(BuildBlobName(q))
            .ExistsAsync();
    }

    public static async Task<bool> BlobExistsAsync<TKey>(
        this IEmptyBlockBlobs<TKey> request,
        TKey key)
    {
        var q = (BlobRequest<TKey, None>)request;

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

    public static async Task<bool> DeleteBlobAsync(
        this IEmptyBlockBlob request)
    {
        var q = (BlobRequest<None, None>)request;

        if (q.BlobContainerClient is null) throw new Exception("BlobContainerClient is not initialized.");

        return await q.BlobContainerClient
            .GetBlobClient(BuildBlobName(q))
            .DeleteIfExistsAsync();
    }

    public static async Task<bool> DeleteBlobAsync<TKey>(
        this IEmptyBlockBlobs<TKey> request,
        TKey key)
    {
        var q = (BlobRequest<TKey, None>)request;

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

    public static async Task<Blob?> GetBlobOrNullAsync(
        this IEmptyBlockBlob request)
    {
        var q = (BlobRequest<None, None>)request;

        return await GetBlobOrNullAsync(q);
    }

    public static async Task<Blob?> GetBlobOrNullAsync<TKey>(
        this IEmptyBlockBlobs<TKey> request,
        TKey key)
    {
        var q = (BlobRequest<TKey, None>)request;

        return await GetBlobOrNullAsync(q, key);
    }

    static async Task<Blob<TValue>?> GetBlobOrNullAsync<TKey, TValue>(
        BlobRequest<TKey, TValue> q,
        TKey? key = default)
    {
        var empty = typeof(TValue) == typeof(None);

        if (q.BlobContainerClient is null) throw new Exception("BlobContainerClient is not initialized.");


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
            response = await blobClient.DownloadAsync(
                conditions: GetBlobRequestConditions(q));

            if (response.GetRawResponse().Status == 304)
            {
                if (q.IfModifiedSince is not null ||
                    q.IfUnmodifiedSince is not null)
                {
                    throw new PreconditionFailedException();
                }

                throw new Exception("Unexpected HTTP 304");
            }
        }
        catch (RequestFailedException rfex) when (rfex.Status == 404)
        {
            if (q.IfExists)
            {
                throw new PreconditionFailedException();
            }

            return null;
        }
        catch (RequestFailedException rfex) when (rfex.Status == 412)
        {
            throw new PreconditionFailedException();
        }

        TValue? value = default;

        if (!empty)
        {
            if (q.ContentSerializer is null) throw new Exception("Serializer is not specified.");

            var pipeline = new ContentPipeline(
                q.ContentSerializer, 
                q.CompressionStrategy);

            value = await pipeline.ReadAndDeserializeAsync<TValue>(response.Value.Content);

            if (value is null)
            {
                return null;
            }
        }
        else
        {
            value = (TValue)(object)None.Instance;
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

    static BlobRequestConditions GetBlobRequestConditions<TKey, TValue>(
        BlobRequest<TKey, TValue> request)
    {
        return new()
        {
            IfMatch = request.IfMatch is not null ?
                new ETag(request.IfMatch) :
                request.IfExists ?
                    ETag.All :
                    null,

            IfNoneMatch = request.IfNotExists ?
                ETag.All :
                null, 

            IfModifiedSince = request.IfModifiedSince is not null ?
                request.IfModifiedSince.Value :
                null,

            IfUnmodifiedSince = request.IfUnmodifiedSince is not null ?
                request.IfUnmodifiedSince.Value :
                null
        };
    }
}
