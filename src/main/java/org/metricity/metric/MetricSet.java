package org.metricity.metric;

import java.util.Deque;
import java.util.Set;

/**
 * A set of supported metrics, accessible by name
 * 
 * @author abutler
 *
 * @param <M>
 *            The subclass of metric
 */
public interface MetricSet<M extends SimpleMetricType<?>> {
	/** @return All metrics supported in this set */
	Set<M> getAllSupported();

	/**
	 * @param metricName
	 *            The name of the metric to get
	 * @return All metrics in this set with the given name
	 */
	Deque<M> get(String metricName);

	/**
	 * @param metric
	 *            The metric to check support for
	 * @return Whether the given metric is supported in this set
	 */
	default boolean isSupported(SimpleMetricType<?> metric) {
		return getAllSupported().contains(metric);
	}
}
