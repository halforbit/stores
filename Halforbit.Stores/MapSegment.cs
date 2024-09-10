namespace Halforbit.Stores;

public struct MapSegment
{
    public ReadOnlyMemory<char> Text { get; init; }
    public int MemberIndex { get; init; }
    public bool IsParameter { get; init; }
    public ReadOnlyMemory<char> Format { get; init; }
}