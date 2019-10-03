package org.metricity.metric.service;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.metricity.anchor.Anchor;
import org.metricity.anchor.AnchorSource;
import org.metricity.anchor.AnchorType;
import org.metricity.metric.Metric;
import org.metricity.metric.MetricSupport;
import org.metricity.metric.MetricType;
import org.observe.ObservableValue;

/** Provides metric data */
public interface MetricChannelService extends MetricQueryService {
	/**
	 * @param <T> The type of the metric
	 * @param anchor The anchor to get metric data for
	 * @param metric The metric to get data for
	 * @param onError The reporter to report reasons for non-support to
	 * @return A MetricChannel which can be used to {@link MetricQueryService#query(MetricChannel, Consumer) query} data for the metric on
	 *         the anchor
	 */
	default <T> MetricChannel<T> getChannel(Anchor anchor, Metric<T> metric, Consumer<String> onError) {
		return getChannel(anchor, metric, false, onError);
	}

	/**
	 * @param <T> The type of the metric
	 * @param anchor The anchor to get metric data for
	 * @param metric The metric to get data for
	 * @param allowRecursion This service prevents circular dependencies unless they are specifically allowed here
	 * @param onError The reporter to report reasons for non-support to
	 * @return A MetricChannel which can be used to {@link MetricQueryService#query(MetricChannel, Consumer) query} data for the metric on
	 *         the anchor
	 */
	<T> MetricChannel<T> getChannel(Anchor anchor, Metric<T> metric, boolean allowRecursion, Consumer<String> onError);

	/**
	 * @param <T> The type of the metric
	 * @param anchor The anchor to get metric data for
	 * @param metric The metric to get data for
	 * @param onError The reporter to report reasons for non-support to
	 * @return A MetricChannel which can be used to {@link MetricQueryService#query(MetricChannel, Consumer) query} data for the metric on
	 *         the anchor
	 */
	default <T> List<? extends MetricChannel<T>> getChannels(Anchor anchor, Metric<T> metric, Consumer<String> onError) {
		return getChannels(anchor, metric, false, onError);
	}

	/**
	 * @param <T> The type of the metric
	 * @param anchor The anchor to get metric data for
	 * @param metric The metric to get data for
	 * @param allowRecursion This service prevents circular dependencies unless they are specifically allowed here
	 * @param onError The reporter to report reasons for non-support to
	 * @return A MetricChannel which can be used to {@link MetricQueryService#query(MetricChannel, Consumer) query} data for the metric on
	 *         the anchor
	 */
	<T> List<? extends MetricChannel<T>> getChannels(Anchor anchor, Metric<T> metric, boolean allowRecursion, Consumer<String> onError);

	/** @return The observable set of metrics that this service currently supports */
	ObservableValue<MetricSupport> getSupport();

	/**
	 * @param anchorSource The anchor source to query support for
	 * @param anchorType The anchor type to query support for
	 * @param metric The metric type to query support for
	 * @return Whether the given metric type is supported by this service
	 */
	boolean isMetricSupported(AnchorSource anchorSource, AnchorType<?> anchorType, MetricType<?> metric);

	/**
	 * @param anchorSource The anchor source to query support for
	 * @param anchorType The anchor type to query support for
	 * @return All metric types that are currently supported by this service for the given anchor source/type
	 */
	Set<MetricType<?>> getAllSupportedMetrics(AnchorSource anchorSource, AnchorType<?> anchorType);

	/**
	 * @param metrics Another component to support some metrics
	 * @param withCaching Whether the derived metric service should support caching, which may greatly improve time performance at the
	 *        expense of memory
	 * @return A channel service whose metric support is the union of this service's and the given component's. Channels returned by the new
	 *         service for metrics supported by both will pull data from the given component.
	 */
	MetricChannelService derived(MetricChannelServiceComponent metrics, boolean withCaching);
}
