package org.metricity.metric.util.derived;

import java.util.Objects;

import org.metricity.anchor.Anchor;
import org.metricity.metric.Metric;
import org.metricity.metric.service.MetricChannel;

/**
 * A {@link MetricDependencyType} resolved for a particular derived metric channel
 * 
 * @param <T> The type of the target metric
 * @param <X> The type of the dependency metric
 */
public class MetricDependency<T, X> {
	private final MetricDependencyType<T, X> theType;
	private final Anchor theAnchor;
	private final Metric<X> theMetric;
	private final boolean isRequired;
	private final MetricChannel<X> theChannel;

	MetricDependency(MetricDependencyType<T, X> type, Anchor anchor, Metric<X> metric, boolean required, MetricChannel<X> channel) {
		if (type == null) {
			throw new NullPointerException();
		} else if (!type.isDynamic() && (anchor == null || metric == null)) {
			throw new NullPointerException();
		}
		theType = type;
		theAnchor = anchor;
		theMetric = metric;
		isRequired = required;
		theChannel = channel;
	}

	/** @return The dependency type */
	public MetricDependencyType<T, X> getType() {
		return theType;
	}

	/** @return The name of this dependency */
	public String getName() {
		return theType.getName();
	}

	/** @return The anchor of this dependency */
	public Anchor getAnchor() {
		return theAnchor;
	}

	/** @return The metric of this dependency */
	public Metric<X> getMetric() {
		return theMetric;
	}

	/** @return Whether this dependency is required to derive the target value */
	public boolean isRequired() {
		return isRequired;
	}

	/** @return The channel resolved for this dependency (may be null if dynamic) */
	public MetricChannel<X> getChannel() {
		return theChannel;
	}

	/** @return Whether this dependency is dynamic */
	public boolean isDynamic() {
		return theType.isDynamic();
	}

	/**
	 * @param ds The static dependency values
	 * @return The needfulness of this dependency
	 */
	public DependencyType isDynamicallyRequired(DependencyValueSet<Anchor, T> ds) {
		return theType.isDynamicallyRequired(ds);
	}

	@Override
	public int hashCode() {
		return Objects.hash(theType.getName(), theChannel);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (!(obj instanceof MetricDependency)) {
			return false;
		}
		MetricDependency<?, ?> other = (MetricDependency<?, ?>) obj;
		return theType.getName().equals(other.theType.getName()) && Objects.equals(theChannel, other.theChannel);
	}

	@Override
	public String toString() {
		return theType.getName() + ": " + theAnchor + "." + theMetric;
	}
}
