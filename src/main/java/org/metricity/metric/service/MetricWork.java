package org.metricity.metric.service;

import java.util.function.Consumer;
import java.util.stream.Stream;

/** A structure that provides information about the current state of metric value computation */
public interface MetricWork {
	/** @return Whether metric value computation is currently executing or scheduled for immediate execution */
	boolean isActive();

	/** @return The notional amount of work that is scheduled */
	double getWaitingWorkAmount();

	/** @return The number of metric channels for which computation is scheduled */
	int getCurrentJobCount();

	/** @return Metric channels for which data is currently being computed */
	Stream<MetricChannel<?>> getCurrentJobs();

	/**
	 * @param active
	 *            The listener to be notified when new work is scheduled after being idle or when all work is completed
	 * @return A runnable to unsubscribe the listener
	 */
	Runnable addActiveListener(Consumer<Boolean> active);
}
