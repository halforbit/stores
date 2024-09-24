namespace Halforbit.Stores;

public class JsonPipelineSerializer : IPipelineSerializer
{
    public void Serialize<TValue>(
        Stream stream,
        TValue value) => System.Text.Json.JsonSerializer.Serialize(stream, value);

    public TValue Deserialize<TValue>(Stream stream) => 
        System.Text.Json.JsonSerializer.Deserialize<TValue>(stream) ?? throw new Exception("Deserialized a null value.");
}
