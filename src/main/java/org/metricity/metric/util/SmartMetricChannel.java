package org.metricity.metric.util;

import java.util.Set;
import java.util.function.Consumer;

import org.metricity.metric.service.MetricChannel;
import org.metricity.metric.service.MetricChannelService;
import org.metricity.metric.service.MetricQueryResult;
import org.metricity.metric.service.MetricQueryService;
import org.metricity.metric.service.MetricTag;

/**
 * A smart metric channel is one that understands and has access to its own data
 *
 * @param <T>
 *            The type of the metric
 */
public interface SmartMetricChannel<T> extends MetricChannel<T> {
	/**
	 * @param depends
	 *            The service to query dependency costs and values from
	 * @return The notional cost of retrieving/computing this channel's data with the given query parameters
	 */
	double getCost(MetricChannelService depends);

	/**
	 * @param time
	 *            The time at which to get the tags
	 * @param depends
	 *            The service to query dependency tags and values from
	 * @return This channel's tags
	 */
	Set<MetricTag> getTags(MetricChannelService depends);

	/**
	 * @param depends The service to query dependency properties from
	 * @return This channel's properties
	 * @see MetricQueryService#isConstant(MetricChannel)
	 */
	boolean isConstant(MetricChannelService depends);

	/**
	 * Queries this channel for data
	 * 
	 * @param depends The service to query dependencies from
	 * @return The results of the query
	 */
	MetricQueryResult<T> query(MetricChannelService depends, Consumer<Object> initialListener);
}
