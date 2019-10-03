package org.metricity.metric.util.derived;

import org.metricity.anchor.Anchor;
import org.metricity.metric.service.MetricChannel;

/**
 * Information on dependency support for a derived metric satisfier that also includes the resolved target anchor/metric and some or all
 * dependency values
 *
 * @param <A> The type of the target anchor
 * @param <T> The type of the target metric
 */
public interface DependencyValueSet<A extends Anchor, T> extends DependencyInstanceSet<A, T> {
	/**
	 * @param <X> The type of the metric for the dependency
	 * @param dependency The name of the dependency
	 * @return The metric channel resolved for the dependency
	 */
	<X> MetricChannel<X> getChannel(String dependency);

	/**
	 * @param <X> The type of the metric for the dependency
	 * @param dependency The name of the dependency
	 * @return The value for the dependency
	 */
	<X> X get(String dependency);

	/**
	 * @param <X> The type of the metric for the dependency
	 * @param dependency The name of the dependency
	 * @param def The value to return if the given dependency is not satisfied
	 * @return The value for the dependency
	 */
	default <X> X get(String dependency, X def) {
		X value = get(dependency);
		if (value == null || (value instanceof Double && Double.isNaN(((Double) value).doubleValue()))) {
			return def;
		}
		return value;
	}
}
