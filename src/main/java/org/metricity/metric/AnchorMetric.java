package org.metricity.metric;

import org.metricity.anchor.Anchor;
import org.metricity.metric.service.MetricResult;
import org.observe.Observable;

/** Represents a set of watched metrics on a single watched anchor */
public interface AnchorMetric extends Comparable<AnchorMetric> {
	/** @return The anchor */
	Anchor getAnchor();

	/**
	 * @param <T>
	 *            The type of the metric
	 * @param metric
	 *            The metric to get the value for
	 * @return The metric result containing the value or computation state for the given metric regarding this anchor
	 */
	<T> MetricResult<T> getResult(Metric<T> metric);
	/**
	 * @param <T>
	 *            The type of the metric
	 * @param metric
	 *            The metric to get the value for
	 * @return The value for the given metric regarding this anchor
	 */
	default <T> T get(Metric<T> metric) {
		return getResult(metric).get();
	}
	/**
	 * @param <T>
	 *            The type of the metric
	 * @param metric
	 *            The metric to get the value for
	 * @param defaultValue
	 *            The value to return if the given metric value is not {@link MetricResult#isAvailable() available}
	 * @return The value for the given metric regarding this anchor
	 */
	default <T> T get(Metric<T> metric, T defaultValue) {
		return getResult(metric).getWithDefault(defaultValue);
	}

	/**
	 * @param metric
	 *            The metric to check
	 * @return Whether the metric for this anchor is supported at the current time
	 */
	default boolean isSupported(Metric<?> metric) {
		return getResult(metric).isValid();
	}
	/**
	 * @param metric
	 *            The metric to check
	 * @return Whether the metric for this anchor has been computed for the current time
	 */
	default boolean isAvailable(Metric<?> metric) {
		return getResult(metric).isAvailable();
	}
	/**
	 * @param metric
	 *            The metric to check
	 * @return Whether the metric for this anchor is scheduled for computation or is currently being computed
	 */
	default boolean isComputing(Metric<?> metric) {
		return getResult(metric).isComputing();
	}

	/** @return An observable that fires whenever the value for any watched metric for this anchor changes */
	Observable<?> updates();
}