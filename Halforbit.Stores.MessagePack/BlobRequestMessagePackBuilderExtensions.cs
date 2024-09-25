namespace Halforbit.Stores;

public static class BlobRequestMessagePackBuilderExtensions
{
    public static ISerializedBlockBlobs MessagePackSerialization(
        this IBlockBlobs request) => (BlobRequest<None, None>)request with
        {
            ContentSerializer = new MessagePackSerializerStrategy(),

            ContentType = "application/x-msgpack",

            ContentTypeExtension = ".msgpack"
        };
}