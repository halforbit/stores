//using Azure;
//using Azure.Storage.Blobs;
using Azure;
using Azure.Storage.Blobs.Models;

namespace Halforbit.Stores;

record BlobPutResult
{
    /// <summary>
    /// The ETag contains a value that you can use to perform operations conditionally. If the request version is 2011-08-18 or newer, the ETag value will be in quotes.
    /// </summary>
    public required string ETag { get; init; }

    /// <summary>
    /// Returns the date and time the container was last modified. Any operation that modifies the blob, including an update of the blob's metadata or properties, changes the last-modified time of the blob.
    /// </summary>
    public required DateTimeOffset LastModified { get; internal set; }

//    /// <summary>
//    /// If the blob has an MD5 hash and this operation is to read the full blob, this response header is returned so that the client can check for message content integrity.
//    /// </summary>
//#pragma warning disable CA1819 // Properties should not return arrays
//    public required byte[] ContentHash { get; internal set; }
//#pragma warning restore CA1819 // Properties should not return arrays

    /// <summary>
    /// A DateTime value returned by the service that uniquely identifies the blob. The value of this header indicates the blob version, and may be used in subsequent requests to access this version of the blob.
    /// </summary>
    public required string VersionId { get; internal set; }

    ///// <summary>
    ///// The SHA-256 hash of the encryption key used to encrypt the blob. This header is only returned when the blob was encrypted with a customer-provided key.
    ///// </summary>
    //public required string EncryptionKeySha256 { get; internal set; }

    ///// <summary>
    ///// Returns the name of the encryption scope used to encrypt the blob contents and application metadata.  Note that the absence of this header implies use of the default account encryption scope.
    ///// </summary>
    //public required string EncryptionScope { get; internal set; }

    ///// <summary>
    ///// The current sequence number for the page blob.  This is only returned for page blobs.
    ///// </summary>
    //public required long BlobSequenceNumber { get; internal set; }

    public static BlobPutResult FromBlobContentInfo(BlobContentInfo blobContentInfo)
    {
        return new()
        { 
            ETag = blobContentInfo.ETag.ToString(),

            LastModified = blobContentInfo.LastModified,

            //ContentHash = blobContentInfo.ContentHash,

            VersionId = blobContentInfo.VersionId,

            //EncryptionKeySha256 = blobContentInfo.EncryptionKeySha256,
            
            //EncryptionScope = blobContentInfo.EncryptionScope,

            //BlobSequenceNumber = blobContentInfo.BlobSequenceNumber
        };
    }
}
