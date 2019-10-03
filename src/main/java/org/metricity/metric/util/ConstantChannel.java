package org.metricity.metric.util;

import java.util.Set;

import org.metricity.anchor.Anchor;
import org.metricity.metric.Metric;
import org.metricity.metric.service.MetricQueryService;
import org.metricity.metric.service.MetricTag;

/**
 * A metric channel defined using {@link HelperMetricServiceComponent.ForMetric#constant(java.util.function.BiFunction)}
 * 
 * @param <T> The type of the metric
 */
public class ConstantChannel<T> extends AbstractMetricChannel<T> implements SingleValuedChannel<T> {
	final T theValue;

	ConstantChannel(MetricQueryService source, Anchor anchor, Metric<T> metric, Set<MetricTag> tags, T value) {
		super(source, anchor, metric, tags);
		theValue = value;
	}

	@Override
	public T getCurrentValue() {
		return theValue;
	}

	static <T> ConstantChannel<T> create(MetricQueryService source, Anchor anchor, Metric<T> metric, Set<MetricTag> tags, T value) {
		return new ConstantChannel<>(source, anchor, metric, tags, value);
	}
}
