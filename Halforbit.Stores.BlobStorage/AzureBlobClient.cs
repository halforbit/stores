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

    public async Task</*Response<*/bool/*>*/> DeleteIfExistsAsync(
        /*DeleteSnapshotsOption snapshotsOption = DeleteSnapshotsOption.None, 
        BlobRequestConditions? conditions = null, 
        CancellationToken cancellationToken = default*/) =>
            await _blobClient.DeleteIfExistsAsync(
                /*snapshotsOption, 
                conditions, 
                cancellationToken*/);

    public async Task</*Response<*/bool/*>*/> ExistsAsync(
        /*CancellationToken cancellationToken = default*/) => 
            await _blobClient.ExistsAsync(/*cancellationToken*/);
}
