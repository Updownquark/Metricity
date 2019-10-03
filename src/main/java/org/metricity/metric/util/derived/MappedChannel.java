package org.metricity.metric.util.derived;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.metricity.anchor.Anchor;
import org.metricity.metric.Metric;
import org.metricity.metric.service.MetricChannel;
import org.metricity.metric.service.MetricChannelService;
import org.metricity.metric.service.MetricQueryResult;
import org.metricity.metric.service.MetricTag;
import org.metricity.metric.util.MetricTimelineWrapper;

public class MappedChannel<X, T> extends AbstractDerivedChannelType.AbstractDerivedChannel<T> {
	private final MetricChannel<X> theSourceChannel;

	public MappedChannel(MappedChannelType<X, T> type, Anchor anchor, Metric<T> metric, MetricChannel<X> sourceChannel) {
		super(type, anchor, metric);
		theSourceChannel = sourceChannel;
	}

	@Override
	public MappedChannelType<X, T> getType() {
		return (MappedChannelType<X, T>) super.getType();
	}

	public MetricChannel<X> getSourceChannel() {
		return theSourceChannel;
	}

	@Override
	public double getCost(MetricChannelService depends) {
		return getType().getDerivationCost() + depends.getCost(theSourceChannel);
	}

	@Override
	public Set<MetricTag> getTags(MetricChannelService depends) {
		return getType().getTags(); // Delegate to the source?
	}

	@Override
	public boolean isConstant(MetricChannelService depends) {
		return depends.isConstant(theSourceChannel);
	}

	@Override
	public MetricQueryResult<T> query(MetricChannelService depends, Consumer<Object> initialListener) {
		Function<X, T> map = getType().getValue() == null ? null : srcValue -> {
			return getType().map(getAnchor(), getMetric(), getSourceChannel(), srcValue);
		};
		return MetricTimelineWrapper.wrap(this, depends.query(theSourceChannel, initialListener), map);
	}

	@Override
	protected int genHashCode() {
		return getType().hashCode() * 31 + super.genHashCode() * 13 + theSourceChannel.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!super.equals(obj)) {
			return false;
		}
		MappedChannel<?, ?> other = (MappedChannel<?, ?>) obj;
		return getType().equals(other.getType()) && theSourceChannel.equals(other.theSourceChannel);
	}
}
