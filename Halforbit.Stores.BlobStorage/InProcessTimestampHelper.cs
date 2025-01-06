//using Azure;
//using Azure.Storage.Blobs;
using System.Diagnostics;

namespace Halforbit.Stores;

static class InProcessTimestampHelper
{
    static readonly DateTime _bootTime = 
        DateTime.UtcNow - TimeSpan.FromMilliseconds(Environment.TickCount64);

    public static DateTime GetTimestamp()
    {        
        TimeSpan elapsedSinceBoot = TimeSpan.FromSeconds(
            (double)Stopwatch.GetTimestamp() / Stopwatch.Frequency);

        return _bootTime + elapsedSinceBoot;
    }
}
