using Azure;
using Azure.Storage.Blobs;
//using Azure.Storage.Blobs.Models;

namespace Halforbit.Stores;

class AzureBlobClient : IBlobClient
{
    readonly BlobClient _blobClient;

    public AzureBlobClient(BlobClient blobClient) =>
        _blobClient = blobClient;

    public IBlobClient WithVersion(
        string versionId) =>
            new AzureBlobClient(
                _blobClient.WithVersion(
                    versionId));

    public Task<Response<bool>> DeleteIfExistsAsync(
        /*DeleteSnapshotsOption snapshotsOption = DeleteSnapshotsOption.None, 
        BlobRequestConditions? conditions = null, 
        CancellationToken cancellationToken = default*/) =>
            _blobClient.DeleteIfExistsAsync(
                /*snapshotsOption, 
                conditions, 
                cancellationToken*/);

    public Task<Response<bool>> ExistsAsync(
        /*CancellationToken cancellationToken = default*/) => 
            _blobClient.ExistsAsync(/*cancellationToken*/);
}
