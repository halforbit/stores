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

    public async Task<BlobPutResult/*Response<BlobContentInfo>*/> UploadAsync(
        Stream content, 
        BlobUploadOptions options/*, 
        CancellationToken cancellationToken = default*/) =>
            BlobPutResult.FromBlobContentInfo(await _blockBlobClient.UploadAsync(
                content, 
                options/*, 
                cancellationToken*/));

    public async Task<BlobGetResult/*Response<BlobDownloadInfo>*/> DownloadAsync(
        /*HttpRange range = default,*/
        BlobRequestConditions? conditions = null/*, 
        bool rangeGetContentHash = false, 
        CancellationToken cancellationToken = default*/) =>
            BlobGetResult.FromBlobDownloadInfoResponse(
                await _blockBlobClient.DownloadAsync(
                    /*range,*/
                    conditions: conditions/*,
                    rangeGetContentHash,
                    cancellationToken*/));

    public Task/*<Response<Blob>>*/ SetMetadataAsync(
        Metadata metadata/*, 
        BlobRequestConditions? conditions = null, 
        CancellationToken cancellationToken = default*/) =>
            _blockBlobClient.SetMetadataAsync(
                metadata/*, 
                conditions, 
                cancellationToken*/);
}
