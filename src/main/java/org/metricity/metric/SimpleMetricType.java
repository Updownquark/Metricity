package org.metricity.metric;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * A metric type that is not either a {@link RelatedMetricType} or a {@link AggregateMetricType}. All metrics are a product of one or more
 * simple metrics.
 * 
 * @param <T> The type of the metric
 */
public interface SimpleMetricType<T> extends MetricType<T> {
	/**
	 * Builds a {@link SimpleMetric}
	 *
	 * @param <T> The type of the metric
	 */
	interface Builder<T> extends MetricType.Builder<T> {
		@Override
		SimpleMetricType<T> getType();

		@Override
		Builder<T> with(String parameterName, Object value) throws IllegalArgumentException;

		@Override
		SimpleMetric<T> build();
	}

	@Override
	Builder<T> build();

	/**
	 * Gets the simple metric type that produces the values for the given metric
	 * 
	 * @param metric The metric to get the terminal metric for
	 * @return The simple metric whose values are most directly related to the values for the given metric
	 */
	static SimpleMetricType<?> getTerminalMetric(MetricType<?> metric) {
		while (!(metric instanceof SimpleMetricType)) {
			if (metric instanceof RelatedMetricType) {
				metric = ((RelatedMetricType<?>) metric).getRelativeMetricType();
			} else if (metric instanceof AggregateMetricType) {
				metric = ((AggregateMetricType<?, ?>) metric).getRelativeMetricType();
			} else {
				throw new IllegalArgumentException("Unrecognized metric type: " + metric.getClass().getName());
			}
		}
		return (SimpleMetricType<?>) metric;
	}

	/**
	 * @param metric The metric to get the components for
	 * @return All {@link SimpleMetricType}s composing the given metric type
	 */
	static List<SimpleMetricType<?>> getSimpleComponents(MetricType<?> metric) {
		class Helper {
			final List<SimpleMetricType<?>> components = new LinkedList<>();

			void addComponents(MetricType<?> m) {
				if (m instanceof SimpleMetricType) {
					components.add((SimpleMetricType<?>) m);
				} else if (m instanceof RelatedMetricType) {
					addComponents(((RelatedMetricType<?>) m).getRelationMetricType());
					addComponents(((RelatedMetricType<?>) m).getRelativeMetricType());
				} else if (m instanceof AggregateMetricType) {
					addComponents(((AggregateMetricType<?, ?>) m).getRelationMetricType());
					addComponents(((AggregateMetricType<?, ?>) m).getRelativeMetricType());
				} else {
					throw new IllegalArgumentException("Unrecognized metric type: " + m.getClass().getName());
				}
			}
		}
		Helper helper = new Helper();
		helper.addComponents(metric);
		return Collections.unmodifiableList(helper.components);
	}
}
