using Azure;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using Microsoft.IO;
using System.Net;
using System.Net.Http.Headers;

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
            BlobTraits.Metadata,
            BlobStates.Version,
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
            key: default, 
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
        if (q.Serializer is null) throw new ArgumentNullException("Serializer is not specified.");

        if (q.ContentSerializer is null) throw new ArgumentNullException("Serializer is not specified.");
        
        using var span = q.Tracer?.StartActiveSpan(nameof(UpsertBlobAsync));

        var blobName = BuildBlobName(q, key);

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
            blobInfo = await q.BlobContainerClient
                .GetBlockBlobClient(blobName)
                .UploadAsync(
                    content: ms, 
                    options: new()
                    {
                        HttpHeaders = blobHttpHeaders,
                        Metadata = metadata
                    });
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

	static async Task<PutResult> UpsertBlobAsyncX<TKey, TValue>(
		BlobRequest<TKey, TValue> q,
		TKey key,
        TValue value,
		Dictionary<string, string>? metadata = null)
	{
        using (var span = q.Tracer?.StartActiveSpan(nameof(UpsertBlobAsync)))
        {
            if (q.Serializer is null) throw new ArgumentNullException("Serializer is not specified.");

            if (q.ContentSerializer is null) throw new ArgumentNullException("Serializer is not specified.");

            var blobName = BuildBlobName(q, key);

            span?.SetAttribute("blob.name", blobName);

            var sasUrl = q.BlobContainerClient
                .GetBlockBlobClient(blobName)
                .GenerateSasUri(
                    BlobSasPermissions.All,
                    DateTimeOffset.UtcNow.AddMinutes(5));

            using var ms = _recyclableMemoryStreamManager.GetStream();

            using (var _ = q.Tracer?.StartActiveSpan("Serialize"))
            {
                var pipeline = new ContentPipeline(q.ContentSerializer, q.CompressionStrategy);

                await pipeline.SerializeAndWriteAsync(value, ms);
            }

            span?.SetAttribute("blob.length", ms.Length);

            //var stream = (Stream)ms;

            //if (q.Compressor is IPipelineCompressor c)
            //{
            //    stream = c.Compress(stream);
            //}

            //q.Serializer.Serialize(stream, value);

            //stream.Flush();

            ms.Seek(0, SeekOrigin.Begin);

            var httpRequestMessage = new HttpRequestMessage(HttpMethod.Put, sasUrl)
            {
                Content = new StreamContent(ms)
            };

            httpRequestMessage.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");

            if (metadata != null)
            {
                foreach (var kvp in metadata)
                {
                    httpRequestMessage.Headers.Add($"x-ms-meta-{kvp.Key}", kvp.Value);
                }
            }

            httpRequestMessage.Headers.Add("x-ms-version", "2024-11-04");

            httpRequestMessage.Headers.Add("x-ms-blob-type", "BlockBlob");

            httpRequestMessage.Headers.Add("x-ms-date", DateTime.UtcNow.ToString("R"));

            var response = default(HttpResponseMessage);

            using (var _ = q.Tracer?.StartActiveSpan("Transmit"))
            {
                var httpClient = q.HttpClientFactory?.CreateClient() ?? new HttpClient();

                response = await httpClient.SendAsync(httpRequestMessage);
            }
            
            try
            {
                response.EnsureSuccessStatusCode();

                var versionId = default(string?);

                if (response.Headers.TryGetValues("x-ms-version-id", out var versionIdValues))
                {
                    versionId = versionIdValues.First();
                }

                if (response.Headers.ETag != null)
                {
                    return new()
                    {
                        Name = blobName,

                        ETag = response.Headers.ETag.Tag,

                        VersionId = versionId
                    };
                }

                throw new InvalidOperationException("ETag not returned in response.");
            }
            finally
            {
                response.Dispose();
            }
        }
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

        var blobName = BuildBlobName(q, key);

        span?.SetAttribute("BlobName", blobName);

        var blobClient = q.BlobContainerClient.GetBlockBlobClient(blobName);

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

    static async Task<Blob<TValue>?> GetBlobOrNullAsyncX<TKey, TValue>(
        BlobRequest<TKey, TValue> q,
        TKey? key = default)
    {
        if (q.BlobContainerClient is null) throw new Exception("BlobContainerClient is not initialized.");

        if (q.Serializer is null) throw new Exception("Serializer is not specified.");

        if (q.ContentSerializer is null) throw new Exception("Serializer is not specified.");

        var blobName = BuildBlobName(q, key);

		var sasUrl = q.BlobContainerClient
            .GetBlockBlobClient(blobName)
            .GenerateSasUri(
                BlobSasPermissions.All, 
                DateTimeOffset.UtcNow.AddMinutes(5));

        var httpRequestMessage = new HttpRequestMessage(HttpMethod.Get, sasUrl);

        httpRequestMessage.Headers.Add("x-ms-version", "2024-11-04");
        
        using var response = await new HttpClient().SendAsync(
            httpRequestMessage, 
            HttpCompletionOption.ResponseHeadersRead);

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }

        response.EnsureSuccessStatusCode();

        var metadata = new Dictionary<string, string>();

        var versionId = default(string?);

        foreach (var header in response.Headers)
        {
            if (header.Key.StartsWith("x-ms-meta-", StringComparison.OrdinalIgnoreCase))
            {
                var metadataKey = header.Key["x-ms-meta-".Length..];
        
                metadata[metadataKey] = string.Join(",", header.Value);
            }
            else if (header.Key == "x-ms-version-id")
            {
                versionId = header.Value.Single();
            }
        }

        var eTag = response.Headers.ETag?.Tag ?? throw new ArgumentException("Response did not include eTag.");

        var creationTime = response.Headers.GetValues("x-ms-creation-time").FirstOrDefault() ?? 
            throw new ArgumentException("Response did not include creation time.");

        var blobType = response.Headers.GetValues("x-ms-blob-type").FirstOrDefault() ?? 
            throw new ArgumentException("Response did not include blob type.");

		var contentStream = await response.Content.ReadAsStreamAsync();

        var pipeline = new ContentPipeline(q.ContentSerializer, q.CompressionStrategy);

        var value = await pipeline.ReadAndDeserializeAsync<TValue>(contentStream);

        if (value is null)
        {
            return null;
        }

        throw new NotImplementedException();

        //return new()
        //{
        //    Name = blobName,
        //    Value = value,
        //    ETag = eTag,
        //    VersionId = versionId,
        //    Metadata = metadata,
        //    CreationTime = DateTime.Parse(creationTime),
        //    BlobType = Enum.Parse<BlobType>(blobType)
        //};
    }

    static string BuildBlobName<TKey, TValue>(
        BlobRequest<TKey, TValue> q, 
        TKey? key = default)
    {
        if (q.KeyMap is not null)
        {
            if (!q.KeyMap.TryMapKeyToString(key, out var name))
            {
                throw new ArgumentException("Could not map key to string.");
            }

            return name;
        }

        return $"{q.Name}{q.ContentTypeExtension}{q.ContentEncodingExtension}";
    }
}
