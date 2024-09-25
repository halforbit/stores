namespace Halforbit.Stores;

public sealed class None
{
    static readonly None _instance = new None();

    None() { }

    public static None Instance => _instance;
};
