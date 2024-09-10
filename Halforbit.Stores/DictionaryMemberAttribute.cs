namespace Halforbit.Stores;

[AttributeUsage(AttributeTargets.Property)]
public class DictionaryMemberAttribute : Attribute
{
    public DictionaryMemberAttribute(
        string? key = null, 
        int majorDigits = 15)
    {
        Key = key;

        MajorDigits = majorDigits;
    }

    public string? Key { get; set; }

    public int MajorDigits { get; set; }
}
