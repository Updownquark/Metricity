package org.metricity.metric;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import com.google.common.reflect.TypeToken;

/**
 * A metric type that produces values that are the result of a relative metric evaluated on an anchor produced by a relation metric
 * 
 * @param <T> The type of the metric
 */
public interface RelatedMetricType<T> extends MetricType<T> {
	/**
	 * A metric parameter type for a {@link RelatedMetricType} or an {@link AggregateMetricType}
	 *
	 * @param <P> The type of the parameter value
	 */
	interface RelationMetricParameter<P> extends MetricParameter<P> {
		/** @return Whether this parameter was specified by the relation or the relative metric */
		boolean isRelative();

		/** @return The metric parameter in either the relation or relative metric represented by this parameter */
		MetricParameter<P> getInternalParam();

		@Override
		default TypeToken<P> getType() {
			return getInternalParam().getType();
		}

		@Override
		default boolean isNullable() {
			return getInternalParam().isNullable();
		}

		@Override
		default Map<String, Predicate<? super P>> getFilters() {
			return getInternalParam().getFilters();
		}

		@Override
		default Optional<P> getDefault() {
			return getInternalParam().getDefault();
		}

		@Override
		default List<P> getAllowedValues() {
			return getInternalParam().getAllowedValues();
		}
	}

	/** @return The type of the relation metric */
	RelationMetricType<?> getRelationMetricType();

	/** @return The type of the relative metric */
	MetricType<T> getRelativeMetricType();

	@Override
	default String getName() {
		return getRelationMetricType().getName() + "." + getRelativeMetricType().getName();
	}

	@Override
	default TypeToken<T> getType() {
		return getRelativeMetricType().getType();
	}

	@Override
	default String getGroupName() {
		return getRelativeMetricType().getGroupName();
	}

	@Override
	default String getDisplayName() {
		return getRelationMetricType().getDisplayName() + " " + getRelativeMetricType().getDisplayName();
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
	Map<String, ? extends RelationMetricParameter<?>> getParameters();

	/**
	 * Builds a {@link RelatedMetric}
	 *
	 * @param <T> The type of the metric
	 */
	interface Builder<T> extends MetricType.Builder<T> {
		@Override
		RelatedMetricType<T> getType();

		@Override
		Builder<T> with(String parameterName, Object value) throws IllegalArgumentException;

		@Override
		RelatedMetric<T> build();
	}

	@Override
	Builder<T> build();

	@Override
	Builder<T> build(MetricParameterMap initParameters);
}
