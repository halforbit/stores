using Azure;
using Azure.Storage.Blobs.Models;

namespace Halforbit.Stores;

record BlobGetResult
{
    static readonly RecyclableDictionaryManager<string, string> _recyclableDictionaryManager =
        new RecyclableDictionaryManager<string, string>();

    public required Stream Content { get; init; }

    public required int Status { get; init; }

    public required IDictionary<string, string>? Metadata { get; init; }

    public required string ETag { get; init; }

    public required string? VersionId { get; init; }

    public required DateTime CreationTime { get; init; }

    public required DateTime LastModified { get; init; }

    public required BlobType BlobType { get; init; }

    public required long ContentLength { get; init; }

    public required string ContentType { get; init; }

    public required string? ContentEncoding { get; init; }

    public required byte[] ContentHash { get; init; }

    public static BlobGetResult FromBlobDownloadInfoResponse(
        Response<BlobDownloadInfo> response)
    {
        var headers = response.GetRawResponse().Headers;

        var metadata = _recyclableDictionaryManager.Get();

        foreach (var header in headers)
        {
            if (header.Name.StartsWith("x-ms-meta-"))
            {
                metadata[header.Name["x-ms-meta-".Length..]] = header.Value;
            }
        }

        return new()
        { 
            Content = response.Value.Content,

            Status = response.GetRawResponse().Status,

            Metadata = metadata,

            ETag = headers.ETag?.ToString() ?? string.Empty,

            VersionId = headers.TryGetValue("x-ms-version-id", out var version) ? 
                version : 
                null,

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
}
