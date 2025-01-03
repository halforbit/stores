using Azure;
//using Azure.Storage.Blobs.Models;

namespace Halforbit.Stores;

interface IBlobClient
{
    IBlobClient WithVersion(string versionId);
 
    Task<Response<bool>> ExistsAsync(
        /*CancellationToken cancellationToken = default*/);

    Task<Response<bool>> DeleteIfExistsAsync(
        /*DeleteSnapshotsOption snapshotsOption = default,
        BlobRequestConditions? conditions = default,
        CancellationToken cancellationToken = default*/);
}
