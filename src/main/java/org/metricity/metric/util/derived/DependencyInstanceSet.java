package org.metricity.metric.util.derived;

import org.metricity.anchor.Anchor;
import org.metricity.metric.Metric;

/**
 * Information on dependency support for a derived metric satisfier that also includes the resolved target anchor/metric
 * 
 * @author abutler
 *
 * @param <A>
 *            The type of the anchor
 * @param <T>
 *            The type of the metric
 */
public interface DependencyInstanceSet<A extends Anchor, T> extends DependencyTypeSet {
	/** @return The anchor to derive metric data for */
	A getAnchor();

	/** @return The metric to derive data for */
	Metric<T> getMetric();
}
