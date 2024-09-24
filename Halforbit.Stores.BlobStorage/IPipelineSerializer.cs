namespace Halforbit.Stores;

public interface IPipelineSerializer
{
    void Serialize<TValue>(Stream stream, TValue value);

    TValue Deserialize<TValue>(Stream stream);
}
