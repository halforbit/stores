using Azure;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using Microsoft.IO;
using System.Net.Http.Headers;

namespace Halforbit.Stores;

public static class BlobRequestOperationExtensions
{
    static RecyclableMemoryStreamManager _recyclableMemoryStreamManager = new RecyclableMemoryStreamManager();

    public static Task<GetBlobResponse<TKey, TValue>> GetAsync<TKey, TValue>(
        this IBlockBlobRequestWithKeyMapValue<TKey, TValue> request,
        TKey key) => throw new NotImplementedException();

    public static Task<TValue?> ValueOrNull<TKey, TValue>(
        this Task<GetBlobResponse<TKey, TValue>> responseTask) => throw new NotImplementedException();

    public static Task<TValue> ValueOrThrow<TKey, TValue>(
        this Task<GetBlobResponse<TKey, TValue>> responseTask) => throw new NotImplementedException();

    public static async Task<TValue?> ValueOrNull<TValue>(
        this Task<GetBlobResponse<TValue>> responseTask) => (await responseTask).Value;

    public static async Task<TValue> ValueOrThrow<TValue>(
        this Task<GetBlobResponse<TValue>> responseTask) => (await responseTask).Value ?? throw new Exception("Requested blob was not found.");

    public static TValue? ValueOrNull<TValue>(
        this GetBlobResponse<TValue> response) => response.Value;

    public static TValue ValueOrThrow<TValue>(
        this GetBlobResponse<TValue> response) => response.Value ?? throw new Exception("Requested blob was not found.");

    public static Task<PutBlobResponse<TKey, TValue>> PutAsync<TKey, TValue>(
        this IBlockBlobRequestWithKeyMapValue<TKey, TValue> request,
        TKey key,
        TValue value) => throw new NotImplementedException();

    //public static async Task<PutBlobResponse<TValue>> UpsertAsync<TValue>(
    //    this IBlockBlobRequestWithSingleValue<TValue> request,
    //    TValue value)
    //{
    //    var q = (BlobRequest<None, TValue>)request;

    //    if (q.Serializer is null) throw new Exception("Serializer is not specified.");

    //    using var stream = await q.BlobContainerClient
    //        .GetBlockBlobClient(BuildBlobName(request))
    //        .OpenWriteAsync(overwrite: true);

    //    q.Serializer.Serialize(stream, value);

    //    return new PutBlobResponse<TValue>();
    //}

    public static async Task<string> UpsertBlobAsync<TValue>(
        this IBlockBlobRequestWithSingleValue<TValue> request,
        TValue value,
        Dictionary<string, string>? metadata = null)
    {
        var q = (BlobRequest<None, TValue>)request;

        if (q.Serializer is null) throw new Exception("Serializer is not specified.");

        if (q.ContentSerializer is null) throw new Exception("Serializer is not specified.");

        var sasUrl = q.BlobContainerClient
            .GetBlockBlobClient(BuildBlobName(request))
            .GenerateSasUri(
                BlobSasPermissions.All,
                DateTimeOffset.UtcNow.AddMinutes(5));


        var pipeline = new ContentPipeline(q.ContentSerializer, q.CompressionStrategy);

        using var ms = _recyclableMemoryStreamManager.GetStream();

        await pipeline.SerializeAndWriteAsync(value, ms);

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

        using var response = await new HttpClient().SendAsync(httpRequestMessage);

        response.EnsureSuccessStatusCode();

        if (response.Headers.ETag != null)
        {
            return response.Headers.ETag.Tag;
        }

        throw new InvalidOperationException("ETag not returned in response.");
    }

    public static async Task<bool> ExistsAsync<TValue>(
        this IBlockBlobRequestWithSingleValue<TValue> request)
    {
        var q = (BlobRequest<None, TValue>)request;

        if (q.BlobContainerClient is null) throw new Exception("BlobContainerClient is not initialized.");

        return await q.BlobContainerClient
            .GetBlobClient(BuildBlobName(request))
            .ExistsAsync();
    }

    public static async Task<bool> DeleteAsync<TValue>(
        this IBlockBlobRequestWithSingleValue<TValue> request)
    {
        var q = (BlobRequest<None, TValue>)request;

        if (q.BlobContainerClient is null) throw new Exception("BlobContainerClient is not initialized.");

        return await q.BlobContainerClient
            .GetBlobClient(BuildBlobName(request))
            .DeleteIfExistsAsync();
    }

    public static async Task<TValue?> GetAsync<TValue>(
        this IBlockBlobRequestWithSingleValue<TValue> request)
    {
        var q = (BlobRequest<None, TValue>)request;

        if (q.Serializer is null) throw new Exception("Serializer is not specified.");

        try
        {
            using var readStream = await q.BlobContainerClient
                .GetBlockBlobClient(BuildBlobName(request))
                .OpenReadAsync();

            var stream = readStream;

            if (q.Compressor is IPipelineCompressor c)
            {
                stream = c.Decompress(stream);
            }

            return q.Serializer.Deserialize<TValue>(stream);
        }
        catch (RequestFailedException rfex) when (rfex.ErrorCode == "BlobNotFound")
        {
            return default;
        }
    }

    public static async Task<Blob<TValue>?> GetBlobOrNullAsync<TValue>(
        this IBlockBlobRequestWithSingleValue<TValue> request)
    {
        var q = (BlobRequest<None, TValue>)request;

        if (q.BlobContainerClient is null) throw new Exception("BlobContainerClient is not initialized.");

        if (q.Serializer is null) throw new Exception("Serializer is not specified.");

        if (q.ContentSerializer is null) throw new Exception("Serializer is not specified.");

        var sasUrl = q.BlobContainerClient
            .GetBlockBlobClient(BuildBlobName(request))
            .GenerateSasUri(
                BlobSasPermissions.All, 
                DateTimeOffset.UtcNow.AddMinutes(5));

        var httpRequestMessage = new HttpRequestMessage(HttpMethod.Get, sasUrl);

        httpRequestMessage.Headers.Add("x-ms-version", "2024-11-04");
        
        using var response = await new HttpClient().SendAsync(
            httpRequestMessage, 
            HttpCompletionOption.ResponseHeadersRead);

        response.EnsureSuccessStatusCode();

        var metadata = new Dictionary<string, string>();
        
        foreach (var header in response.Headers)
        {
            if (header.Key.StartsWith("x-ms-meta-", StringComparison.OrdinalIgnoreCase))
            {
                var metadataKey = header.Key["x-ms-meta-".Length..];
        
                metadata[metadataKey] = string.Join(",", header.Value);
            }
        }

        var contentStream = await response.Content.ReadAsStreamAsync();

        var pipeline = new ContentPipeline(q.ContentSerializer, q.CompressionStrategy);

        var value = await pipeline.ReadAndDeserializeAsync<TValue>(contentStream);

        if (value is null)
        {
            return null;
        }

        return new()
        {
            Value = value,
            Info = new BlobInfo()
        };

        //var stream = contentStream;

        //if (q.Compressor is IPipelineCompressor c)
        //{
        //    stream = c.Decompress(stream);
        //}

        //return new()
        //{
        //    Value = q.Serializer.Deserialize<TValue>(stream),
            
        //    Info = new BlobInfo()
        //};
    }

    static string BuildBlobName<TValue>(IBlockBlobRequestWithSingleValue<TValue> request)
    {
        var q = (BlobRequest<None, TValue>)request;

        return $"{q.Key}{q.ContentTypeExtension}{q.ContentEncodingExtension}";
    }
}
