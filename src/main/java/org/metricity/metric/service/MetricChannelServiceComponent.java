package org.metricity.metric.service;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import org.metricity.anchor.Anchor;
import org.metricity.anchor.AnchorSource;
import org.metricity.anchor.AnchorType;
import org.metricity.metric.Metric;
import org.metricity.metric.MetricSupport;
import org.metricity.metric.SimpleMetricType;
import org.observe.Observable;

/** A component that supplies some metric values */
public interface MetricChannelServiceComponent extends MetricQueryService {
	/**
	 * @param depends
	 *            The service to query dependencies with
	 */
	void initialize(MetricQueryService depends);

	/** @return The relative priority of this component */
	int getPriority();

	/**
	 * @param anchorSource
	 *            The anchor source to query support for
	 * @param anchorType
	 *            The anchor type to query support for
	 * @param dependencySupport
	 *            The metric support for this component's dependencies
	 * @return The set of metrics supported for the given anchor source/type
	 */
	Collection<SimpleMetricType<?>> getAllSupportedMetrics(AnchorSource anchorSource, AnchorType<?> anchorType,
		MetricSupport dependencySupport);

	/** @return An observable that fires whenever this component's support changes */
	Observable<?> supportChanges();

	/**
	 * @param <T>
	 *            the type of the metric
	 * @param anchor
	 *            The anchor to get channels for
	 * @param metric
	 *            The metric to get channels for
	 * @param dependencies
	 *            The service to query dependencies
	 * @param onError
	 *            The reporter to report reasons for non-support to
	 * @return The list of metric channels that this component can support for the given anchor/metric
	 */
	<T> List<? extends MetricChannel<T>> getChannels(Anchor anchor, Metric<T> metric, MetricChannelService dependencies,
			Consumer<String> onError);

	/**
	 * @param <T>
	 *            the type of the metric
	 * @param anchor
	 *            The anchor to get the channel for
	 * @param metric
	 *            The metric to get the channel for
	 * @param dependencies
	 *            The service to query dependencies
	 * @param onError
	 *            The reporter to report reasons for non-support to
	 * @return The metric channel that this component can support for the given anchor/metric, or null if not supported
	 */
	<T> MetricChannel<T> getChannel(Anchor anchor, Metric<T> metric, MetricChannelService dependencies, Consumer<String> onError);
}
