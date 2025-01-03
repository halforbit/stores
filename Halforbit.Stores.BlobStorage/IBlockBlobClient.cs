using Azure;
using Azure.Storage.Blobs.Models;
using Metadata = System.Collections.Generic.IDictionary<string, string>;

namespace Halforbit.Stores;

interface IBlockBlobClient
{
    IBlockBlobClient WithVersion(string versionId);

    Task<Response<BlobContentInfo>> UploadAsync(
        Stream content,/*
         * HttpHeaders
         * Metadata
         * Conditions
         * - IfMatch
         * - IfNoneMatch
         * - IfModifiedSince
         * - IfUnmodifiedSince
        */BlobUploadOptions options/*,
        CancellationToken cancellationToken = default*/);

    Task<Response<BlobDownloadInfo>> DownloadAsync(
        /*HttpRange range = default,
         * - IfMatch
         * - IfNoneMatch
         * - IfModifiedSince
         * - IfUnmodifiedSince
        */BlobRequestConditions? conditions = default/*, 
        bool rangeGetContentHash = default,
        CancellationToken cancellationToken = default*/);

    Task<Response<BlobInfo>> SetMetadataAsync(
        Metadata metadata/*,
        BlobRequestConditions? conditions = default,
        CancellationToken cancellationToken = default*/);
}
