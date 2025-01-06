namespace Halforbit.Stores;

public class ActionFailedException : Exception
{
    public ActionFailedException(string message, Exception? inner) : base(message, inner) { }
}
