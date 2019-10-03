package org.metricity.metric;

import java.util.Map;

import org.metricity.anchor.Anchor;
import org.metricity.metric.RelatedMetricType.RelationMetricParameter;

/**
 * A metric whose value is that of a relative metric evaluated on an anchor produced by a {@link RelationMetricType relation} metric
 * 
 * @param <T> The type of the metric
 */
public interface RelatedMetric<T> extends Metric<T> {
	/**
	 * A metric parameter value for a {@link RelatedMetric} or an {@link AggregateMetric}
	 *
	 * @param <P> The type of the parameter value
	 */
	interface RelationMetricParameterValue<P> extends MetricParameterValue<P> {
		@Override
		RelationMetricParameter<P> getParameter();

		/** @return The metric from the relation or relative metric */
		MetricParameterValue<P> getInternalParam();
	}

	@Override
	RelatedMetricType<T> getType();

	/** @return The relation metric */
	Metric<? extends Anchor> getRelationMetric();

	/** @return The relative metric */
	Metric<T> getRelativeMetric();

	@Override
	Map<String, ? extends RelationMetricParameterValue<?>> getParameters();
}
