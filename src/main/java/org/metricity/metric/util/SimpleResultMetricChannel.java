package org.metricity.metric.util;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import org.metricity.anchor.Anchor;
import org.metricity.metric.Metric;
import org.metricity.metric.service.MetricQueryOptions;
import org.metricity.metric.service.MetricQueryResult;
import org.metricity.metric.service.MetricQueryService;
import org.metricity.metric.service.MetricResult;
import org.metricity.metric.service.MetricTag;
import org.observe.Observable;
import org.observe.Observer;
import org.observe.SimpleObservable;
import org.observe.Subscription;
import org.qommons.Identifiable;
import org.qommons.Transaction;

public abstract class SimpleResultMetricChannel<T> extends AbstractMetricChannel<T> {
	protected final SimpleResultChannelCache theCache;
	protected final SimpleObservable<Object> theUpdates;
	final AtomicInteger theUseCount;
	private volatile boolean isCached;

	volatile long theStamp;

	SimpleResultMetricChannel(SimpleResultChannelCache cache, MetricQueryService source, Anchor anchor, Metric<T> metric,
		Set<MetricTag> tags) {
		super(source, anchor, metric, tags);
		theCache = cache;

		// the updates observable has to specify "internalState", which will prevent observers added during the firing of updates
		// from receiving those updates
		theUpdates = new SimpleObservable<Object>(null, true, true);
		theUseCount = new AtomicInteger();
	}

	protected void setCached(boolean cached) {
		isCached = cached;
	}

	protected boolean isCached() {
		return isCached;
	}

	abstract double getCost();

	abstract boolean isConstant();

	/**
	 * @param actions Accepts actions to perform after the change
	 * @param cause The cause of the change
	 */
	abstract void updated(Consumer<Runnable> actions, Object cause);

	protected void update(Object update) {
		theStamp++;
		theUpdates.onNext(update);
	}

	public <O> Observable<O> register(String operation, Function<SimpleResultMetricChannel<T>, Observable<O>> observable) {
		return new Observable<O>() {
			private Object theIdentity;

			@Override
			public Object getIdentity() {
				return Identifiable.wrap(SimpleResultMetricChannel.this, operation);
			}

			@Override
			public Subscription subscribe(Observer<? super O> observer) {
				if (theUseCount.getAndIncrement() == 0) {
					// Ensure this channel is cached
					SimpleResultMetricChannel<T> channel = theCache.ensureCached(SimpleResultMetricChannel.this);
					if (channel != SimpleResultMetricChannel.this) {
						// If there was an identical channel cached that is not this channel, use the one in the cache instead
						theUseCount.getAndDecrement();
						return channel.register(operation, observable).subscribe(observer);
					}
				}
				Subscription sub = observable.apply(SimpleResultMetricChannel.this).subscribe(observer);
				return new Subscription() {
					private boolean unsubscribed;

					@Override
					public void unsubscribe() {
						if (!unsubscribed) {
							unsubscribed = true;
							theUseCount.decrementAndGet();
							sub.unsubscribe();
						}
					}
				};
			}

			@Override
			public boolean isSafe() {
				return observable.apply(SimpleResultMetricChannel.this).isSafe();
			}

			@Override
			public Transaction lock() {
				return observable.apply(SimpleResultMetricChannel.this).lock();
			}

			@Override
			public Transaction tryLock() {
				return observable.apply(SimpleResultMetricChannel.this).tryLock();
			}
		};
	}

	public MetricQueryResult<T> query(Consumer<Object> initialListener) {
		SimpleResultMetricChannel<T> commsChannel = theCache.ensureCached(SimpleResultMetricChannel.this);
		if (commsChannel != this)
			return commsChannel.query(initialListener);

		SimpleMetricResults<T> result = createResult();
		SimpleMetricQueryResult<T> qRes = new SimpleMetricQueryResult<>(this, result);
		if (initialListener != null)
			qRes.subscription = qRes.notifyOnChange(initialListener);
		return qRes;
	}

	abstract SimpleMetricResults<T> createResult();

	interface SimpleMetricResults<T> {
		Transaction lock();

		Transaction tryLock();

		MetricResult<T> getResult(MetricResult<T> result);
	}

	static class SimpleMetricQueryResult<T> extends MetricQueryResult.AbstractMQR<T> {
		final SimpleMetricResults<T> theResults;
		Subscription subscription;

		SimpleMetricQueryResult(SimpleResultMetricChannel<T> channel, SimpleMetricResults<T> results) {
			super(channel);
			theResults = results;
		}

		@Override
		public SimpleResultMetricChannel<T> getChannel() {
			return (SimpleResultMetricChannel<T>) super.getChannel();
		}

		@Override
		public Transaction lock() {
			return theResults.lock();
		}

		@Override
		public Transaction tryLock() {
			return theResults.tryLock();
		}

		@Override
		public long getStamp() {
			return getChannel().theStamp;
		}

		@Override
		public void checkDynamicDependencies() {}

		@Override
		public MetricResult<T> getResult(MetricResult<T> result, MetricQueryOptions options) {
			return theResults.getResult(result);
		}

		@Override
		public Subscription notifyOnChange(Consumer<Object> onChange) {
			return getChannel().register("results", channel -> channel.theUpdates).act(onChange);
		}

		@Override
		public boolean isValid() {
			return true;
		}

		@Override
		public void unsubscribe() {
			Subscription sub = subscription;
			if (sub != null) {
				subscription = null;
				sub.unsubscribe();
			}
		}
	}
}
