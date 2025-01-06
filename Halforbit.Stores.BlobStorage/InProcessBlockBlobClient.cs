//using Azure;
//using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.IO;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

namespace Halforbit.Stores;

class InProcessBlockBlobClient : IBlockBlobClient
{
    static readonly RecyclableMemoryStreamManager _recyclableMemoryStreamManager = 
        new RecyclableMemoryStreamManager();

    readonly string _blobName;
    
    readonly string? _versionId;
    
    readonly InMemoryBlobContainer _blobContainer;

    public InProcessBlockBlobClient(
        string blobName,
        string? versionId,
        InMemoryBlobContainer blobContainer)
    {
        _blobName = blobName;
        
        _versionId = versionId;
        
        _blobContainer = blobContainer;
    }

    public IBlockBlobClient WithVersion(
        string versionId)
    {
        return new InProcessBlockBlobClient(
            _blobName,
            versionId,
            _blobContainer);
    }

    public async Task<BlobPutResult/*Response<BlobContentInfo>*/> UploadAsync(
        Stream content, 
        BlobUploadOptions options)
    {
        var start = Stopwatch.GetTimestamp();

        var timestamp = InProcessTimestampHelper.GetTimestamp();

        var memoryStream = new MemoryStream();

        content.CopyTo(memoryStream);

        var contentArray = memoryStream.ToArray();

        var blobPutResult = new BlobPutResult
        {
            ETag = GenerateETag(
                timestamp,
                contentArray,
                options.Metadata),

            LastModified = new DateTimeOffset(timestamp),

            VersionId = GenerateVersionId(timestamp)
        };

        var inMemoryBlob = _blobContainer.Blobs.GetOrAdd(
            _blobName, 
            k => new InMemoryBlob
            {
                BlobName = _blobName,

                Versions = []
            });

        var ifMatch = options.Conditions.IfMatch;
        
        var ifNoneMatch = options.Conditions.IfNoneMatch;

        if (ifMatch is not null || ifNoneMatch is not null)
        {
            var latest = inMemoryBlob.Versions.Values
                .OrderByDescending(v => v.Blob.LastModified)
                .FirstOrDefault();

            var latestETag = latest?.Blob.ETag;

            if (ifMatch is not null)
            {
                if ((ifMatch.Value.ToString() != "*" && ifMatch.ToString() != latestETag) ||
                    latestETag is null)
                {
                    throw new PreconditionFailedException();
                }
            }

            if (ifNoneMatch is not null)                
            {
                if (ifNoneMatch.ToString() == latestETag ||
                    (ifNoneMatch.ToString() == "*" && inMemoryBlob.Versions.Any()))
                {
                    throw new PreconditionFailedException();
                }
            }
        }

        var ifModifiedSince = options.Conditions.IfModifiedSince;

        if (ifModifiedSince is not null)
        {
            if (inMemoryBlob.Versions.IsEmpty ||
                !(inMemoryBlob.Versions
                    .OrderByDescending(v => v.Value.Blob.LastModified)
                    .First().Value.Blob.LastModified > ifModifiedSince.Value.DateTime))
            {
                throw new PreconditionFailedException();
            }
        }

        var ifUnmodifiedSince = options.Conditions.IfUnmodifiedSince;

        if (ifUnmodifiedSince is not null)
        {
            if (inMemoryBlob.Versions.Any(
                v => v.Value.Blob.LastModified > ifUnmodifiedSince.Value.DateTime))
            {
                throw new PreconditionFailedException();
            }
        }
                
        var blob = new Blob 
        {
            Name = _blobName,
            
            ETag = blobPutResult.ETag,
            
            VersionId = blobPutResult.VersionId,
            
            Metadata = options.Metadata?.ToDictionary(kv => kv.Key, kv => kv.Value),
            
            CreationTime = timestamp,
            
            LastModified = timestamp,
            
            BlobType = BlobType.BlockBlob,
            
            ContentLength = contentArray.Length,
            
            ContentType = string.Empty,
            
            ContentEncoding = null,
            
            ContentHash = []
        };

        if (!inMemoryBlob.Versions.TryAdd(
            blobPutResult.VersionId, 
            new InMemoryBlobVersion
            {
                Blob = blob,

                Content = contentArray
            }))
        {
            throw new ArgumentException("Version already exists.");
        }

        await InProcessDelay.SimulateDelayAsync(
            Stopwatch.GetElapsedTime(start),
            1,
            contentArray.Length);

        return blobPutResult;
    }

    public async Task<BlobGetResult?/*Response<BlobDownloadInfo>*/> DownloadAsync(
        BlobRequestConditions? conditions = null)
    {
        var start = Stopwatch.GetTimestamp();

        var (_, version, isRoot) = ResolveBlobVersion(
            _blobContainer, 
            _blobName, _versionId);

        if (conditions is not null)
        {
            var ifMatch = conditions.IfMatch;

            if (ifMatch is not null)
            {
                if (version is null)
                {
                    throw new PreconditionFailedException();
                }

                if (ifMatch.ToString() != "*" &&  ifMatch.ToString() != version.Blob.ETag)
                {
                    throw new PreconditionFailedException();
                }
            }
        }

        if (version is null)
        {
            return null;
        }
        var blob = version.Blob;

        var content = version.Content;

        var contentStream = _recyclableMemoryStreamManager.GetStream();

        contentStream.Write(
            content.ToArray(), 
            0, 
            content.Length);

        contentStream.Seek(0, SeekOrigin.Begin);

        await InProcessDelay.SimulateDelayAsync(
            Stopwatch.GetElapsedTime(start),
            1,
            blob.ContentLength);

        return new()
        { 
            Content = contentStream,

            Status = 200, 

            Metadata = blob.Metadata,

            ETag = blob.ETag,

            VersionId = blob.VersionId,

            CreationTime = blob.CreationTime,

            LastModified = blob.LastModified,

            BlobType = blob.BlobType,

            ContentLength = blob.ContentLength,

            ContentType = blob.ContentType,

            ContentEncoding = blob.ContentEncoding,

            ContentHash = blob.ContentHash
        };
    }

    public async Task/*<Response<BlobInfo>>*/ SetMetadataAsync(
        IDictionary<string, string> metadata)
    {
        var start = Stopwatch.GetTimestamp();

        var (blob, version, isRoot) = ResolveBlobVersion(
            _blobContainer,
            _blobName,
            _versionId);

        if (blob is null || version is null)
        {
            throw new ActionFailedException("Cannot set metadata because blob does not exist.", null);
        }

        if (!isRoot)
        {
            throw new ActionFailedException("Cannot set metadata on a non-root blob.", null);
        }

        var versionId = version.Blob.VersionId ?? throw new ArgumentException("Blob does not have a version id.");

        blob.Versions[versionId] = version with
        {
            Blob = version.Blob with { Metadata = metadata }
        };

        await InProcessDelay.SimulateDelayAsync(
            Stopwatch.GetElapsedTime(start),
            1,
            0);
    }

    static (InMemoryBlob? Blob, InMemoryBlobVersion? Version, bool IsRoot) ResolveBlobVersion(
        InMemoryBlobContainer container, 
        string blobName,
        string? versionId)
    {
        if (!container.Blobs.TryGetValue(
            blobName, 
            out var blob))
        {
            return (null, null, false);
        }

        var versions = blob.Versions;

        if (versions.IsEmpty)
        {
            return (null, null, false);
        }

        var latest = versions
            .OrderByDescending(v => v.Value.Blob.LastModified)
            .First().Value;

        if (versionId is not null)
        {
            if (versions.TryGetValue(versionId, out var version))
            {
                return (
                    Blob: blob,
                    Version: version, 
                    IsRoot: version.Blob.VersionId == latest.Blob.VersionId);
            }

            return (null, null, false);
        }

        return (
            Blob: blob,
            Version: latest, 
            IsRoot: true);
    }

    /// <summary>
    /// Generates an ETag for a blob based on its timestamp, content, and metadata.
    /// </summary>
    /// <param name="timestamp">The timestamp of the blob.</param>
    /// <param name="content">The byte array representing the blob's content.</param>
    /// <param name="metadata">Optional metadata associated with the blob.</param>
    /// <returns>An Azure.ETag representing the ETag.</returns>
    static string GenerateETag(
        DateTime timestamp, 
        byte[] content, 
        IDictionary<string, string>? metadata)
    {
        using var sha256 = SHA256.Create();

        // Combine timestamp, content, and metadata into a single byte array.
        var timestampBytes = Encoding.UTF8.GetBytes(timestamp.ToString("o")); // ISO 8601 format.
        var metadataBytes = metadata != null
            ? Encoding.UTF8.GetBytes(string.Join("|", metadata.OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}={kv.Value}")))
            : Array.Empty<byte>();

        var combinedBytes = timestampBytes
            .Concat(content)
            .Concat(metadataBytes)
            .ToArray();

        // Compute the hash.
        var hashBytes = sha256.ComputeHash(combinedBytes);

        // Convert the hash to a hexadecimal string.
        var etagString = BitConverter.ToString(hashBytes).Replace("-", string.Empty);

        return etagString;
    }

    static string GenerateVersionId(DateTime timestamp)
    {
        using var sha256 = SHA256.Create();

        // Convert the timestamp to ISO 8601 format and encode as bytes.
        var timestampBytes = Encoding.UTF8.GetBytes(timestamp.ToString("o"));

        // Compute the hash of the timestamp.
        var hashBytes = sha256.ComputeHash(timestampBytes);

        // Convert the hash to a Base64 string.
        return Convert.ToBase64String(hashBytes);
    }
}
