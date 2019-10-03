package org.metricity.metric.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;

import org.metricity.anchor.Anchor;
import org.metricity.anchor.types.WorldAnchor;
import org.metricity.metric.Metric;
import org.metricity.metric.service.MetricQueryService;
import org.qommons.Transaction;
import org.qommons.collect.OptimisticContext;

public class SimpleResultChannelCache {
	public interface Locker {
		Transaction lock();

		long getStamp();

		boolean checkStamp(long stamp);

		default <T> T doOptimistically(Function<OptimisticContext, T> operation) {
			long structStamp = getStamp();
			if (structStamp != 0) {
				boolean[] keepTrying = new boolean[] { true };
				keepTrying[0] = false;
				T res = operation.apply( //
						() -> {
							if (!checkStamp(structStamp)) {
								keepTrying[0] = true;
								return false;
							}
							return true;
						});
				if (!keepTrying[0]) {
					return res;
				}
			}
			try (Transaction t = lock()) {
				return operation.apply(OptimisticContext.TRUE);
			}
		}
	}

	private static class CacheKey {
		final MetricQueryService theSource;
		final Anchor theAnchor;
		final Metric<?> theMetric;
		private final int hashCode;

		CacheKey(MetricQueryService source, Anchor anchor, Metric<?> metric) {
			theSource = source;
			theAnchor = anchor;
			theMetric = metric;
			hashCode = theAnchor.hashCode() + theMetric.hashCode();
		}

		@Override
		public int hashCode() {
			return hashCode;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			} else if (!(obj instanceof CacheKey) || hashCode != obj.hashCode()) {
				return false;
			}
			CacheKey other = (CacheKey) obj;
			return theSource == other.theSource && theAnchor.equals(other.theAnchor) && theMetric.equals(other.theMetric);
		}

		@Override
		public String toString() {
			return theSource + ": " + theAnchor + "." + theMetric;
		}
	}

	private static class WorldData implements Locker {
		final ConcurrentLinkedQueue<SimpleResultMetricChannel<?>> theCachedResults;
		final ReentrantReadWriteLock theLock;
		final AtomicLong theChanges;
		volatile boolean isChanging;

		WorldData() {
			theCachedResults = new ConcurrentLinkedQueue<>();
			theLock = new ReentrantReadWriteLock();
			theChanges = new AtomicLong(1);
		}

		@Override
		public Transaction lock() {
			Lock lock = theLock.readLock();
			lock.lock();
			return lock::unlock;
		}

		@Override
		public long getStamp() {
			if (isChanging) {
				return 0;
			}
			return theChanges.get();
		}

		@Override
		public boolean checkStamp(long stamp) {
			return stamp != 0 && theChanges.get() == stamp;
		}
	}

	private final ReentrantReadWriteLock theChannelLock;
	private final Map<CacheKey, SimpleResultMetricChannel<?>> theChannels;
	private final ConcurrentHashMap<WorldAnchor, WorldData> theCachedResults;

	public SimpleResultChannelCache() {
		theChannelLock = new ReentrantReadWriteLock();
		theChannels = new HashMap<>();
		theCachedResults = new ConcurrentHashMap<>();
	}

	public void check(WorldAnchor world, Object cause) {
		WorldData results = theCachedResults.get(world);
		if (results != null) {
			Lock lock = results.theLock.writeLock();
			lock.lock();
			try {
				results.isChanging = true;
				results.theChanges.getAndIncrement();
				Iterator<SimpleResultMetricChannel<?>> resultIter = results.theCachedResults.iterator();
				LinkedList<Runnable> actions = new LinkedList<>();
				while (resultIter.hasNext())
					resultIter.next().updated(actions::add, cause);
				for (Runnable action : actions)
					action.run();
			} finally {
				results.isChanging = false;
				lock.unlock();
			}
		}
	}

	public <T> SimpleResultMetricChannel<T> createOrRetrieve(MetricQueryService source, Anchor anchor, Metric<T> metric,
			Supplier<SimpleResultMetricChannel<T>> creator) {
		CacheKey key = new CacheKey(source, anchor, metric);
		Lock lock = theChannelLock.readLock();
		lock.lock();
		try {
			SimpleResultMetricChannel<T> channel = (SimpleResultMetricChannel<T>) theChannels.get(key);
			if (channel != null) {
				return channel;
			}
			lock.unlock();
			lock = null;
			channel = creator.get();
			if (channel != null) {
				lock = theChannelLock.writeLock();
				lock.lock();
				SimpleResultMetricChannel<T> cachedChannel = (SimpleResultMetricChannel<T>) theChannels.get(key);
				if (cachedChannel != null) {
					return cachedChannel;
				}
				theCachedResults.computeIfAbsent(anchor.getWorld(), id -> new WorldData()).theCachedResults.add(channel);
				theChannels.put(key, channel);
				channel.setCached(true);
			}
			return channel;
		} finally {
			if (lock != null) {
				lock.unlock();
			}
		}
	}

	<T> SimpleResultMetricChannel<T> ensureCached(SimpleResultMetricChannel<T> channel) {
		Lock lock = theChannelLock.readLock();
		lock.lock();
		try {
			if (channel.isCached()) {
				return channel;
			}
			CacheKey key = new CacheKey(channel.getSource(), channel.getAnchor(), channel.getMetric());
			SimpleResultMetricChannel<T> cached = (SimpleResultMetricChannel<T>) theChannels.get(key);
			if (cached != null) {
				return cached;
			}
			lock.unlock();
			lock = theChannelLock.writeLock();
			lock.lock();
			cached = (SimpleResultMetricChannel<T>) theChannels.get(key);
			if (cached != null) {
				return cached;
			}
			theChannels.put(key, channel);
			channel.setCached(true);
			theCachedResults.computeIfAbsent(channel.getAnchor().getWorld(), id -> new WorldData()).theCachedResults.add(channel);
			return channel;
		} finally {
			lock.unlock();
		}
	}

	public void clear() {
		throw new IllegalStateException("Not implemented");
	}

	public Locker lockFor(Anchor anchor) {
		return theCachedResults.computeIfAbsent(anchor.getWorld(), w -> new WorldData());
	}
}
