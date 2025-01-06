namespace Halforbit.Stores;

static class InProcessDelay
{
    static readonly Random _random = new Random();

    const double MinLatency = 0.005;

    const double MaxLatency = 0.015;

    const double BytesPerSecond = 1_000_000_000;

    public static async Task SimulateDelayAsync(
        TimeSpan runTime, 
        int requestsSent,
        long bytesSent)
    {
        var latencySeconds = 0d;

        for (var i = 0; i < requestsSent; i++)
        {
            latencySeconds += Sample(MinLatency, MaxLatency, _random);
        }

        var transmitSeconds = bytesSent / BytesPerSecond;

        var delayTime = TimeSpan.FromSeconds(latencySeconds + transmitSeconds);

        if (delayTime > runTime)
        {
            await Task.Delay(delayTime - runTime);
        }
        else
        {
            throw new TimeoutException("Run time took longer than delay to simulate.");
        }
    }

    static double Sample(double min, double max, Random random)
    {
        if (min >= max)
        {
            throw new ArgumentException("The minimum value must be less than the maximum value.");
        }

        // Calculate the mean and standard deviation
        double mean = (min + max) / 2;
        double stdDev = (max - min) / 6; // 99.7% of values fall within ±3 standard deviations

        // Generate a sample using the Box-Muller transform
        double u1 = random.NextDouble();
        double u2 = random.NextDouble();
        double standardNormal = Math.Sqrt(-2.0 * Math.Log(u1)) * Math.Cos(2.0 * Math.PI * u2);

        // Scale and shift the sample to fit the desired distribution
        double sample = mean + standardNormal * stdDev;

        // Clamp the sample to be within the specified range
        return Math.Max(min, Math.Min(max, sample));
    }
}
