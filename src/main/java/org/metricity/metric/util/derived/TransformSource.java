package org.metricity.metric.util.derived;

import org.metricity.anchor.Anchor;
import org.metricity.metric.Metric;
import org.metricity.metric.service.MetricChannel;

/**
 * A singular metric dependency for a mapped channel
 * 
 * @param <A> The type of the target anchor
 * @param <X> The type of the dependency metric
 * @param <T> the type of the target metric
 */
public interface TransformSource<A extends Anchor, X, T> {
	/** @return The target anchor */
	A getAnchor();

	/** @return The target metric */
	Metric<T> getMetric();

	/** @return The channel for the dependency */
	MetricChannel<X> getChannel();

	/** @return The value of the dependency */
	X get();
}
