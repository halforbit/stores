using Azure;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Metadata = System.Collections.Generic.IDictionary<string, string>;

namespace Halforbit.Stores;

class AzureBlockBlobClient : IBlockBlobClient
{
    readonly BlockBlobClient _blockBlobClient;

    public AzureBlockBlobClient(
        BlockBlobClient blockBlobClient) => 
            _blockBlobClient = blockBlobClient;

    public IBlockBlobClient WithVersion(
        string versionId) => 
            new AzureBlockBlobClient(
                _blockBlobClient.WithVersion(
                    versionId));

    public Task<Response<BlobDownloadInfo>> DownloadAsync(
        /*HttpRange range = default,*/
        BlobRequestConditions? conditions = null/*, 
        bool rangeGetContentHash = false, 
        CancellationToken cancellationToken = default*/) =>
            _blockBlobClient.DownloadAsync(
                /*range,*/
                conditions: conditions/*,
                rangeGetContentHash,
                cancellationToken*/);

    public Task<Response<BlobInfo>> SetMetadataAsync(
        Metadata metadata/*, 
        BlobRequestConditions? conditions = null, 
        CancellationToken cancellationToken = default*/) =>
            _blockBlobClient.SetMetadataAsync(
                metadata/*, 
                conditions, 
                cancellationToken*/);

    public Task<Response<BlobContentInfo>> UploadAsync(
        Stream content, 
        BlobUploadOptions options/*, 
        CancellationToken cancellationToken = default*/) =>
            _blockBlobClient.UploadAsync(
                content, 
                options/*, 
                cancellationToken*/);
}
