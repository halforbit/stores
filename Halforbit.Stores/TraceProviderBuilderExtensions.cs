using OpenTelemetry.Trace;

namespace Halforbit.Stores;

public static class TraceProviderBuilderExtensions
{
	public static TracerProviderBuilder AddHalforbitStoresInstrumentation(
		this TracerProviderBuilder builder)
	{
		builder.AddSource("Halforbit.Stores");

		return builder;
	}
}