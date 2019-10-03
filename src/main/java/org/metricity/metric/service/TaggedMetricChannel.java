package org.metricity.metric.service;

import java.util.Set;

/**
 * A metric channel that knows its own tags
 * 
 * @author abutler
 *
 * @param <T>
 *            The type of the metric
 */
public interface TaggedMetricChannel<T> extends MetricChannel<T> {
	/** @return This channel's tag set */
	Set<MetricTag> getTags();
}
