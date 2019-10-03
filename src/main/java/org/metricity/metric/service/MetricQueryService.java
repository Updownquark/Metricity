package org.metricity.metric.service;

import java.util.Set;
import java.util.function.Consumer;

import org.observe.ObservableValue;

/** Supports queries of metric data given a {@link MetricChannel} */
public interface MetricQueryService {
	/**
	 * @param channel The channel to get the calculation cost for
	 * @return A rough estimation (in seconds) of the cost per datum of result calculation for this channel at the given time
	 */
	double getCost(MetricChannel<?> channel);

	/**
	 * @param channel The metric channel to get the tags for
	 * @param time The time at which to get the tags on the channel
	 * @return The channel's tags at the given time
	 */
	Set<MetricTag> getTags(MetricChannel<?> channel);

	/**
	 * @param channel The channel to test
	 * @return Whether the given channel produces values that cannot change
	 */
	boolean isConstant(MetricChannel<?> channel);

	/**
	 * <p>
	 * Executes a query against metric data for a channel.
	 * </p>
	 * 
	 * <p>
	 * A connection created by {@link org.observe.Observable#subscribe(org.observe.Observer) subscribing} to the result's
	 * {@link ObservableValue#changes() changes} will reach into the channel's data source and/or dependencies to be updated with a new
	 * result when changes are detected.
	 * </p>
	 * 
	 * @param <T> The type of the metric
	 * @param channel The channel to query data for
	 * @return The observable result containing the status and value for the given channel
	 */
	<T> MetricQueryResult<T> query(MetricChannel<T> channel, Consumer<Object> initialListener);
}
