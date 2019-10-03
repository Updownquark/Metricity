package org.metricity.metric;

import java.util.Collection;

import org.metricity.anchor.Anchor;

/**
 * A RelationMetric based on a multiplicitous (one- or many-to-many) anchor relationship
 *
 * @param <T> The type of the relative metric
 * @param <X> The type of the aggregate metric
 */
public interface AggregateMetric<T, X> extends Metric<X> {
	@Override
	AggregateMetricType<T, X> getType();

	/** @return The metric supplying the collection of anchors related to the target anchor */
	Metric<? extends Collection<? extends Anchor>> getRelationMetric();

	/** @return The metric to evaluate and then aggregate for each related anchor */
	Metric<T> getRelativeMetric();
}
