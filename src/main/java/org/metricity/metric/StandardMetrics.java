package org.metricity.metric;

public class StandardMetrics {
	/** An anchor's name */
	public static final SimpleMetric<String> NAME = MetricTypeBuilder.build("Name", String.class).build().build().build();

	/** The existence of an anchor in the world */
	public static final SimpleMetric<Boolean> EXISTS = MetricTypeBuilder.build("Exists", Boolean.class).noUI().build().build().build();
	/** A metric to define when an anchor is of interest within the world. The value of this metric is not as important as it's range. */
	public static final SimpleMetric<Boolean> OF_INTEREST = MetricTypeBuilder.build("Of Interest", Boolean.class).noUI().build().build()
		.build();

	/** Whether an anchor is active, by some notion, in the scenario (e.g. a transmitter is transmitting) */
	public static final SimpleMetric<Boolean> ACTIVE = MetricTypeBuilder.build("Active", Boolean.class).noUI().build().build().build();
}
