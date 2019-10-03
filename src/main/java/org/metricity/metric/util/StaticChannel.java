package org.metricity.metric.util;

import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.metricity.anchor.Anchor;
import org.metricity.metric.Metric;
import org.metricity.metric.service.MetricQueryService;
import org.metricity.metric.service.MetricResult;
import org.metricity.metric.service.MetricTag;
import org.qommons.Transaction;

/**
 * A channel defined using {@link HelperMetricServiceComponent.ForMetric#statically(Function, java.util.function.BiFunction, Function)}
 * 
 * @param <A> The type of the anchor
 * @param <T> The type of the metric
 */
public class StaticChannel<A, T> extends SimpleResultMetricChannel<T> implements SingleValuedChannel<T> {
	private final A theAnchorObj;
	private final Function<? super A, ? extends T> theAnchorValue;
	private final Function<T, T> theCopier;

	/** The value for the channel as of the last update */
	protected T theCachedValue;

	StaticChannel(SimpleResultChannelCache cache, MetricQueryService source, Anchor anchor, Metric<T> metric, Set<MetricTag> tags, //
		A anchorObj, Function<? super A, ? extends T> anchorValue, Function<T, T> copy) {
		super(cache, source, anchor, metric, tags);
		theAnchorObj = anchorObj;
		theAnchorValue = anchorValue;
		theCopier = copy;

		theCachedValue = theCopier.apply(theAnchorValue.apply(theAnchorObj));
	}

	@Override
	public T getCurrentValue() {
		return theCachedValue;
	}

	@Override
	double getCost() {
		return 0;
	}

	@Override
	boolean isConstant() {
		return false;
	}

	@Override
	void updated(Consumer<Runnable> actions, Object cause) {
		theStamp++;
		T newValue = theAnchorValue.apply(theAnchorObj);
		if (!Objects.equals(theCachedValue, newValue)) {
			theCachedValue = theCopier.apply(newValue);
			actions.accept(() -> update(cause));
		}
	}

	@Override
	SimpleMetricResults<T> createResult() {
		return new StaticMetricResults<>(this);
	}

	private static class StaticMetricResults<T> implements SimpleMetricResults<T> {
		protected final StaticChannel<?, T> theChannel;

		StaticMetricResults(StaticChannel<?, T> channel) {
			theChannel = channel;
		}

		@Override
		public Transaction lock() {
			return Transaction.NONE;
		}

		@Override
		public Transaction tryLock() {
			return Transaction.NONE;
		}

		@Override
		public MetricResult<T> getResult(MetricResult<T> result) {
			return result.setValue(theChannel.theCachedValue);
		}
	}

	static <A, T> StaticChannel<A, T> create(SimpleResultChannelCache cache, MetricQueryService source, Anchor anchor, Metric<T> metric,
		Set<MetricTag> tags, A anchorObj, Function<? super A, ? extends T> value, Function<T, T> copy) {
		return (StaticChannel<A, T>) cache.createOrRetrieve(source, anchor, metric,
			() -> new StaticChannel<A, T>(cache, source, anchor, metric, tags, anchorObj, value, copy));
	}
}
