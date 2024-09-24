namespace Halforbit.Stores;

public static class BlobRequestBuilderExtensions
{
    public static ISerializedBlockBlob MessagePackSerialization(
        this IBlockBlob request) => (BlobRequest<None, None>)request with
        {
            ContentSerializer = new MessagePackSerializerStrategy(),

            ContentType = "application/x-msgpack",

            ContentTypeExtension = ".msgpack"
        };
}