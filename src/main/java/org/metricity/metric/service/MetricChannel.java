package org.metricity.metric.service;

import org.metricity.anchor.Anchor;
import org.metricity.metric.Metric;

/**
 * Represents a source of data for a single anchor-metric pair
 * 
 * @param <T>
 *            The type of the metric
 */
public interface MetricChannel<T> {
	/** @return The anchor that this channel is for */
	Anchor getAnchor();

	/** @return The metric that this channel is for */
	Metric<T> getMetric();

	/** @return Whether this channel may have itself as an eventual dependency (adaptive metrics) */
	default boolean isRecursive() {
		return false;
	}
}
