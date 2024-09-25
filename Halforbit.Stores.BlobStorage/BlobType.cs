namespace Halforbit.Stores;

public enum BlobType : byte
{
    Unknown = 0,
    BlockBlob = 1,
    AppendBlob = 2,
    PageBlob = 3
}
