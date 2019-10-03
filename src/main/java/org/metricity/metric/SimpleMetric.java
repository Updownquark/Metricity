package org.metricity.metric;

/**
 * An instance of a {@link SimpleMetricType}
 * 
 * @param <T> The type of the metric
 */
public interface SimpleMetric<T> extends Metric<T> {
	@Override
	SimpleMetricType<T> getType();
}
