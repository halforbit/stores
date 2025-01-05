namespace Halforbit.Stores;

public class PreconditionFailedException : Exception
{
}

public class ActionFailedException : Exception
{
    public ActionFailedException(string message, Exception? inner) : base(message, inner) { }
}
