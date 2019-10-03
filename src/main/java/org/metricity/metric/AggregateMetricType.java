package org.metricity.metric;

import java.util.Map;

import org.metricity.metric.RelatedMetricType.RelationMetricParameter;

import com.google.common.reflect.TypeToken;

/**
 * A metric based on a multiplicitous (one- or many-to-many) anchor relationship
 *
 * @param <T> The type of the relative metric
 * @param <X> The type of the aggregate metric
 */
public interface AggregateMetricType<T, X> extends MetricType<X> {
	/** @return The metric supplying the anchor or collection of anchors related to the target anchor */
	MultiRelationMetricType<?, ?> getRelationMetricType();

	/** @return The metric to evaluate and then aggregate for each related anchor */
	MetricType<T> getRelativeMetricType();

	/** @return The aggregation to use on the relative metric values for each related anchor */
	MetricAggregation<? super T, X> getAggregation();

	@Override
	Map<String, ? extends RelationMetricParameter<?>> getParameters();

	@Override
	default TypeToken<X> getType() {
		return getAggregation().getType();
	}

	@Override
	default String getGroupName() {
		return getRelativeMetricType().getGroupName();
	}

	@Override
	default String getDisplayName() {
		return getAggregation() + "(" + getRelationMetricType().getDisplayName() + " " + getRelativeMetricType().getDisplayName() + ")";
	}

	@Override
	default boolean isInternalOnly() {
		return getRelativeMetricType().isInternalOnly();
	}

	@Override
	default boolean isUI() {
		return getRelativeMetricType().isUI();
	}

	@Override
	default String getName() {
		return getAggregation() + "(" + getRelationMetricType().getName() + "." + getRelativeMetricType().getName() + ")";
	}

	/**
	 * Builds an {@link AggregateMetric}
	 *
	 * @param <T> The type of the relative metric
	 * @param <X> The type of the aggregate metric
	 */
	interface Builder<T, X> extends MetricType.Builder<X> {
		@Override
		AggregateMetricType<T, X> getType();

		@Override
		Builder<T, X> with(String parameterName, Object value) throws IllegalArgumentException;

		@Override
		AggregateMetric<T, X> build();
	}

	@Override
	Builder<T, X> build();

	@Override
	Builder<T, X> build(MetricParameterMap initParameters);
}
