package org.metricity.metric.service.impl;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.dbug.DBug;
import org.dbug.DBugAnchor;
import org.dbug.DBugAnchorType;
import org.metricity.metric.service.MetricChannel;
import org.metricity.metric.service.MetricQueryOptions;
import org.metricity.metric.service.MetricQueryResult;
import org.metricity.metric.service.MetricQueryService;
import org.metricity.metric.service.MetricResult;
import org.observe.Subscription;
import org.observe.util.TypeTokens;
import org.qommons.Causable;
import org.qommons.Identifiable;
import org.qommons.Transaction;
import org.qommons.ValueHolder;
import org.qommons.collect.ListenerList;

import com.google.common.reflect.TypeToken;

/**
 * <p>
 * This class maintains a cache of metric timelines so that the tree of all the metrics needed to satisfy a set of top-level metrics and
 * their dependencies becomes a graph, allowing re-use of memory structures and computation results. This feature is absolutely critical for
 * performance in Sage.
 * </p>
 * <p>
 * The graph of timelines supports circularity, which is needed for adaptive metrics.
 * </p>
 * <p>
 * This class is also helpful in debugging and profiling, since (currently) all timelines are gathered centrally here.
 * </p>
 */
public class MetricTimelineCache {
	private static final boolean DEBUG_SAMPLE_CACHING_OFF = false;
	private static final boolean DEBUG_VALUE_CACHING_OFF = false;

	private static class MetricChannelKey<T> {
		final MetricChannel<T> channel;
		final int channelHash;

		MetricChannelKey(MetricChannel<T> channel) {
			this(channel, channel.hashCode() * 43);
		}

		private MetricChannelKey(MetricChannel<T> channel, int channelHash) {
			this.channel = channel;
			this.channelHash = channelHash;
		}

		@Override
		public int hashCode() {
			return channelHash;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			} else if (!(o instanceof MetricChannelKey)) {
				return false;
			}
			MetricChannelKey<?> other = (MetricChannelKey<?>) o;
			if (channelHash != other.channelHash) {
				return false;
			}
			return channel.equals(other.channel);
		}

		@Override
		public String toString() {
			return channel.toString();
		}
	}

	final ConcurrentHashMap<MetricChannelKey<?>, CachedMetricResultHolder<?>> theResultCache;
	volatile int theSupportStamp;
	volatile int theQueryCount;

	/** Creates the cache */
	public MetricTimelineCache() {
		theResultCache = new ConcurrentHashMap<>(25000, .75f, Runtime.getRuntime().availableProcessors());
	}

	/** Called when metric support changes */
	public void clearValueCache() {
		theSupportStamp++;
	}

	/**
	 * @param <T> The type of the metric
	 * @param srcChannel The metric channel to get or create a cached result for
	 * @param supplier The implementation that can create a result to supply the actual metric data
	 * @param updateReceiver
	 * @return A common result supplying the metric's data that is shared with all other consumers of the same data
	 */
	public <T> MetricQueryResult<T> query(MetricChannel<T> srcChannel, Consumer<Object> updateReceiver, MetricQueryService supplier) {
		MetricChannelKey<T> channelKey = new MetricChannelKey<>(srcChannel);
		return _query(channelKey, updateReceiver, ur -> supplier.query(srcChannel, ur));
	}

	<T> MetricResultWrapper<T> _query(MetricChannelKey<T> channel, Consumer<Object> updateReceiver,
		Function<Consumer<Object>, MetricQueryResult<T>> actualQuery) {
		CachedMetricResult<T> timeline = getCachedResult(channel, actualQuery);
		/* The time series now keep only weak references to their timelines
		 * (because the timeline reference graph may be cyclic due to adaptive metrics),
		 * so it's possible for a time series to be emptied out of timelines without its knowledge.
		 * It would thus become orphaned within the cache.
		 * So from time to time, it's good to run through the whole cache and clear such orphaned series.
		 * We can't do this very often though, since this operation's time is proportional to the number of timelines in the cache. */
		int qc = ++theQueryCount;
		if (qc % 1000 == 0 && qc >= theResultCache.size()) {
			theQueryCount = 0;
			Iterator<CachedMetricResultHolder<?>> serieses = theResultCache.values().iterator();
			while (serieses.hasNext()) {
				CachedMetricResultHolder<?> series = serieses.next();
				if (series.check()) {
					serieses.remove();
				}
			}
		}
		return wrap(timeline, updateReceiver);
	}

	private <T> MetricResultWrapper<T> wrap(CachedMetricResult<T> cachedResult, Consumer<Object> updateReceiver) {
		return new MetricResultWrapper<>(cachedResult, updateReceiver);
	}

	private <T> CachedMetricResult<T> getCachedResult(MetricChannelKey<T> channelKey,
		Function<Consumer<Object>, MetricQueryResult<T>> actualQuery) {
		ValueHolder<CachedMetricResult<T>> timeline = new ValueHolder<>();
		BiFunction<MetricChannelKey<?>, CachedMetricResultHolder<?>, CachedMetricResultHolder<?>> computer = (key,
			previous) -> compute(channelKey, previous, actualQuery != null, timeline);
		theResultCache.compute(channelKey, computer);
		if (timeline.isPresent()) {
			timeline.get().execute(actualQuery);
		}
		return timeline.get();
	}

	private <T> CachedMetricResultHolder<?> compute(MetricChannelKey<T> channelKey, CachedMetricResultHolder<?> previous, boolean create,
		Consumer<CachedMetricResult<T>> timeline) {
		CachedMetricResult<T> t = null;
		CachedMetricResultHolder<T> timelines = null;
		if (previous != null) {
			timelines = (CachedMetricResultHolder<T>) previous;
			t = timelines.getTimeline(create);
		}
		if (t == null && create) {
			timelines = new CachedMetricResultHolder<>(channelKey);
			t = timelines.getTimeline(true);
		}
		if (t != null) {
			timeline.accept(t);
		}
		return timelines;
	}

	static final int STARTED_MASK = 0b1;
	static final int UPDATING_MASK = 0b10;
	static final int INITIALIZING_MASK = 0b100;
	static final int UPDATES_BLOCKED_MASK = 0b1000;
	static final int VALID = 0b11;
	static final int INVALID = 0b10;

	/**
	 * <p>
	 * The purpose of this class is to hold a reference, strong or weak, to a {@link CachedMetricResult}. The reference is weak most of the
	 * time, but a strong reference is held when a result is being searched for, created, or purged as a result of
	 * {@link MetricResultTimeline#unsubscribe() unsubscription}.
	 * </p>
	 * <p>
	 * The reason for making the reference weak most of the time was because of adaptive metric channels. These channels create a cycle
	 * within the cache, e.g. A refers to B refers to C refers to A again. This had the effect that even when all the channels were
	 * unsubscribed <i>externally</i>, the circular <i>internal</i> subscriptions A->B->C->A were never unsubscribed, because the timeline
	 * was held as long as there was at least 1 active subscription. This caused a memory leak, where these cycles were persistent within
	 * the cache.
	 * </p>
	 * <p>
	 * Distinguishing between subscriptions that external code cares about and subscriptions making up the cycle turns out to be extremely
	 * difficult. Making the references to the timelines weak here allowed them to be garbage collected by the VM, eliminating the leak.
	 * </p>
	 * <p>
	 * TODO Unfortunately, something I didn't count on is that weak reference {@link WeakReference#get() get} calls turn out to be quite
	 * slow, on the order of 3µs. This has caused a completely unacceptable slow down to the system.
	 * </p>
	 * <p>
	 * I'm preparing this branch for cold storage at the moment, so it's probably not the time to address this, but when this gets picked
	 * back up, it will be necessary to find an alternate solution to the circular problem. Some possibilities:
	 * <ul>
	 * <li>Add a strong reference to the query key so that the search can be done without calling WeakReference.get() unless the timeline is
	 * actually needed.</li>
	 * <li>Make an abstraction of this class with strong and weak extensions and only use the weak extension for channels that may be
	 * circular</li>
	 * <li>BEST: Figure out a reliable way to detect when all external subscriptions to a timeline have been released</li>
	 * </ul>
	 * </p>
	 *
	 * @param <T> The type of the metric
	 */
	static class CachedResultReference<T> extends WeakReference<CachedMetricResult<T>> {
		CachedMetricResult<T> hardReference;

		CachedResultReference(CachedMetricResult<T> referent) {
			super(referent);
		}

		boolean hold() {
			hardReference = get();
			return hardReference != null;
		}

		void release() {
			hardReference = null;
		}
	}

	final class CachedMetricResultHolder<T> {
		final MetricChannelKey<T> channelKey;
		private volatile CachedResultReference<T> timelines;

		CachedMetricResultHolder(MetricChannelKey<T> channelKey) {
			this.channelKey = channelKey;
		}

		CachedMetricResult<T> getTimeline(boolean create) {
			synchronized (this) {
				CachedResultReference<T> copy = timelines;
				if (copy == null) {
					return null;
				}
				// First, obtain a hard reference so the GC can't mess with us
				CachedMetricResult<T> found = purgeClearedResult(copy, true);
				if (found != null) {
					found.hold();
				} else if (create) {
					copy = new CachedResultReference<>(found = new CachedMetricResult<>(this));
				} else {
					found = null;
				}
				copy.release();
				return found;
			}
		}

		boolean releaseTimeline(CachedMetricResult<T> timeline) {
			synchronized (this) {
				CachedResultReference<T> copy = timelines;
				if (copy == null) {
					// Not sure this can happen, but pretty sure this is a safe response
					return false;
				}
				CachedMetricResult<T> result = purgeClearedResult(copy, true);
				if (result != null && result != timeline)
					timelines = null;
				copy.release();
				return timelines == null;
			}
		}

		boolean check() {
			synchronized (this) {
				CachedResultReference<T> copy = timelines;
				if (copy == null) {
					// Not sure this can happen, but pretty sure this is a safe response
					return false;
				}
				CachedMetricResult<T> result = purgeClearedResult(copy, false);
				if (result == null) {
					timelines = null;
				}
				return result == null;
			}
		}

		@Override
		public String toString() {
			return channelKey.toString();
		}
	}

	static <T> CachedMetricResult<T> purgeClearedResult(CachedResultReference<T> copy, boolean harden) {
		CachedMetricResult<T> result = copy.get();
		if (result != null) {
			if (harden) {
				copy.hardReference = result;
			}
		}
		return result;
	}

	final DBugAnchorType<CachedMetricResult<?>> CMT_DEBUG = DBug.declare("metricity",
		(Class<CachedMetricResult<?>>) (Class<?>) MetricTimelineCache.CachedMetricResult.class, b -> b//
			.withStaticField("channel", new TypeToken<MetricChannel<?>>() {}, CachedMetricResult::getChannel)//
			.withStaticField("systemId", TypeTokens.get().INT, System::identityHashCode)//
			.withEvent("create", null)//
			.withEvent("initialize", null)//
			.withEvent("preInitialized", null)//
			.withEvent("initUpdate", null)//
			.withEvent("tryUpdate", eb -> eb.withEventField("blockReason", TypeTokens.get().STRING))//
			.withEvent("useCachedValue", eb -> eb.withEventField("cache", new TypeToken<MetricResultCacheValue<?>>() {}))//
			.withEvent("calculating", null)//
			.withEvent("calculated",
				eb -> eb.withEventField("result", new TypeToken<MetricResultCacheValue<?>>() {}).withEventField("hasResult",
					TypeTokens.get().BOOLEAN))//
			.withEvent("transferCache", eb -> eb//
				.withEventField("target", new TypeToken<CachedMetricResult<?>>() {})//
				.withEventField("cache", new TypeToken<MetricResultCacheValue<?>>() {}))//
	);

	/* CachedMetricTimeline's lifecycle:
	 * When a CachedMetricTimeline (CMT) is created, its wanted variable is initialized with 1. Any time someone new asks for the timeline
	 * from the cache, its wanted variable is incremented by the hold() method.
	 * In the case of a caller that does not care to listen to the timeline for updates, the wanted variable is decremented as soon as the
	 * timeline is returned.  Otherwise, the wanted variable is decremented when the listener is unsubscribed.
	 * A CMT's wanted variable can never be increased from zero.  After the wanted variable is zero'd, the timeline cannot be re-used even
	 * if it is retrieved from the cache before it can be removed.
	 *
	 * A CMT itself is unsubscribed when wanted==0 and the timeline is complete, i.e.:
	 * * The last listener is unsubscribed after the timeline is complete
	 * * The wrapper timeline is returned to the last interested queryer (no listeners)
	 * * The timeline completes and no one is listening for updates
	 */
	class CachedMetricResult<T> implements MetricQueryResult<T>, Consumer<Object> {
		private final CachedMetricResultHolder<T> holder;
		private final AtomicInteger theComputationState;
		private final ListenerList<Consumer<Object>> theUpdates;

		private Thread updatingThread;
		final AtomicInteger wanted;

		private volatile MetricQueryResult<T> theResult;
		private volatile Subscription theUpdateSub;
		private volatile int theResultState;
		/** The most recent data point retrieved, for caching */
		private volatile MetricResultCacheValue<T> theValueCache;

		private TypeToken<MetricResult<T>> theType;
		private Object theIdentity;
		private volatile long theStamp;

		final DBugAnchor<CachedMetricResult<?>> debug;

		CachedMetricResult(CachedMetricResultHolder<T> holder) {
			this.holder = holder;
			theComputationState = new AtomicInteger();

			theUpdates = ListenerList.build().forEachSafe(false).withFastSize(false)//
				.withSyncType(ListenerList.SynchronizationType.LIST)//
				.build();

			wanted = new AtomicInteger(1);

			debug = CMT_DEBUG.debug(this).build();
			debug.event("create").occurred();
		}

		boolean hold() {
			return wanted.updateAndGet(old -> old > 0 ? old + 1 : old) > 0;
		}

		void execute(Function<Consumer<Object>, MetricQueryResult<T>> supplier) {
			if (theComputationState.compareAndSet(0, STARTED_MASK | INITIALIZING_MASK)) {
				try (Transaction initT = debug.event("initialize").begin()) {
					theResult = supplier.apply(this);
					theUpdateSub = theResult.notifyOnChange(this);
					if (!theUpdates.isEmpty()) {
						do {
							Causable initialization = Causable.simpleCause("init");
							try (Transaction initUpdateT = debug.event("initUpdate").begin(); //
								Transaction initEventT = Causable.use(initialization)) {
								theComputationState.set(STARTED_MASK | INITIALIZING_MASK); // Clear the blocked updates flag
								accept(initialization);
							}
						} while ((theComputationState.get() & UPDATES_BLOCKED_MASK) != 0);
					}
					theComputationState.set(STARTED_MASK); // Clear the initializing flag
				}
			} else {
				debug.event("preInitialized").occurred();
			}
		}

		@Override
		public Subscription notifyOnChange(Consumer<Object> onChange) {
			class ListenerRemove implements Subscription {
				Runnable updateRemove;

				ListenerRemove() {
					updateRemove = theUpdates.add(onChange, false);
				}

				@Override
				public void unsubscribe() {
					updateRemove.run();
					updateRemove = null;
					if (wanted.decrementAndGet() == 0) {
						// Nobody's listening anymore
						unsubscribe();
					}
				}
			}
			ListenerRemove remove = new ListenerRemove();
			// Wait just a minute for the timeline
			// Iit's better to return a populated timeline rather than firing a bunch of updates when it arrives
			int tries = 0;
			while (theResult == null && (tries++) < 10) {
				try {
					Thread.sleep(2);
				} catch (InterruptedException e) {}
			}
			return remove;
		}

		@Override
		public void accept(Object cause) {
			theStamp++;
			MetricQueryResult<T> result = theResult;
			if (result != null) {
				// System.out.println(Thread.currentThread() + ": Updates for " + timeSeries.channelKey.channel);
				boolean invalid = !result.isValid();
				MetricResultCacheValue<T> cache = theValueCache;
				if (cache != null && (invalid || cache.supportStamp != theSupportStamp)) {
					cache = theValueCache = null;
				}
				theResultState = invalid ? INVALID : 0;
				int mask = startUpdateFiring();
				if (mask != 0) {
					theUpdates.forEach(//
						p -> p.accept(cause));
					theComputationState.set(mask & ~UPDATING_MASK);
					updatingThread = null;
				}
			}
		}

		private int startUpdateFiring() {
			if (holder.channelKey.channel.getMetric().getName().equals("Adapted Data Rate")) { // DEBUG
				System.currentTimeMillis();
			}
			// Don't fire multiple updates/progress simultaneously
			Thread currentThread = Thread.currentThread();
			int mask = theComputationState.getAndUpdate(old -> old | UPDATING_MASK);
			if (mask == STARTED_MASK) {} else if (updatingThread == currentThread) {
				debug.event("tryUpdate").with("blockReason", "recursion").occurred();
				if ((mask & UPDATING_MASK) == 0) {
					theComputationState.getAndUpdate(old -> old & ~UPDATING_MASK);
				}
				return 0;// Prevent recursive updates
			} else if ((mask & INITIALIZING_MASK) != 0) {
				// Until the timeline has been initialized, other updates should be blocked
				if ((mask & UPDATING_MASK) != 0) {
					// During the initial update firing stage, it's possible that some listeners would act on the initial update
					// before this update happened, so we need to re-fire the initial update after it finishes
					theComputationState.compareAndSet(mask, mask | UPDATES_BLOCKED_MASK);
					debug.event("tryUpdate").with("blockReason", "refire").occurred();
					return 0;
				}
			} else {
				// Initialization has completed, but the update lock is already held, so we'll just wait our turn
				do {
					try {
						Thread.sleep(2);
					} catch (InterruptedException e) {}
				} while (!theComputationState.compareAndSet(STARTED_MASK, STARTED_MASK | UPDATING_MASK));
			}
			debug.event("tryUpdate").with("blockReason", "none").occurred();
			updatingThread = currentThread;
			return mask;
		}

		@Override
		public MetricChannel<T> getChannel() {
			return holder.channelKey.channel;
		}

		@Override
		public Object getIdentity() {
			if (theIdentity == null)
				theIdentity = Identifiable.wrap(getChannel(), "result");
			return theIdentity;
		}

		@Override
		public TypeToken<MetricResult<T>> getType() {
			if (theType == null)
				theType = MetricResult.TYPE_KEY.getCompoundType(getChannel().getMetric().getType().getType());
			return theType;
		}

		@Override
		public long getStamp() {
			return theStamp;
		}

		private int getState(int stateStamp, int stateMask) {
			return Integer.bitCount(stateStamp & stateMask);
		}

		@Override
		public boolean isValid() {
			MetricQueryResult<T> timeline = theResult;
			if (timeline == null) {
				return true;
			}
			int stamp = theResultState;
			switch (getState(stamp, VALID)) {
			case 0: // Unknown
				break;
			case 1: // Known invalid
				return false;
			case 2: // Known valid
				return true;
			}
			boolean valid = timeline.isValid();
			stamp |= (valid ? VALID : INVALID);
			if (!DEBUG_VALUE_CACHING_OFF) {
				theResultState = stamp;
			}
			return valid;
		}

		private MetricQueryResult<T> getResult() {
			MetricQueryResult<T> result = theResult;
			if (result == null) {
				do {
					try {
						Thread.sleep(2);
					} catch (InterruptedException e) {}
				} while ((result = theResult) == null);
			}
			return result;
		}

		@Override
		public Transaction lock() {
			return getResult().lock();
		}

		@Override
		public Transaction tryLock() {
			MetricQueryResult<T> result = theResult;
			if (result == null)
				return null;
			return result.tryLock();
		}

		@Override
		public MetricResult<T> waitFor(long wait) {
			MetricResultCacheValue<T> cache = theValueCache;
			if (cache != null) {
				if (cache.supportStamp != theSupportStamp) {
					cache = theValueCache = null;
				} else
					return cache; // Already available
			}

			MetricQueryResult<T> timeline = theResult;
			if (timeline == null) {
				long time = System.currentTimeMillis();
				do {
					try {
						Thread.sleep(2);
					} catch (InterruptedException e) {}
				} while ((timeline = theResult) == null && System.currentTimeMillis() < time + wait);
				wait -= System.currentTimeMillis() - time;
			}
			if (timeline != null) {
				return timeline.waitFor(wait);
			} else
				return MetricResult.COMPUTING();
		}

		void removeFromCache() {
			// If this is the cached timeline, remove it
			boolean removeFromMap = holder.releaseTimeline(this);
			if (removeFromMap) {
				theResultCache.compute(holder.channelKey, (k, ts) -> ts == holder ? null : ts);
			}
		}

		@Override
		public void unsubscribe() {
			theResult.unsubscribe();
			removeFromCache();
		}

		@Override
		public void checkDynamicDependencies() {
			MetricQueryResult<T> timeline = theResult;
			if (timeline != null) {
				timeline.checkDynamicDependencies();
			}
		}

		@Override
		public MetricResult<T> getResult(MetricResult<T> result, MetricQueryOptions options) {
			if (options.isUseCache()) {
				MetricResultCacheValue<T> cache = theValueCache;
				if (cache != null) {
					if (cache.supportStamp != theSupportStamp) {
						theValueCache = null;
					} else {
						debug.event("useCachedValue").with("cache", cache::copy).occurred();
						return result.setState(cache);
					}
				}
			}
			MetricQueryResult<T> myResult = theResult;
			if (myResult == null) {
				debug.event("calculated").with("result", result::copy).with("hasTimeline", false).occurred();
				return result.unavailable(true);
			}
			try (Transaction t = debug.event("calculating").begin()) {
				myResult.getResult(result, options);
			}
			if (!DEBUG_VALUE_CACHING_OFF) {
				if (result.isCachable()) {
					theValueCache = new MetricResultCacheValue<>(theSupportStamp, result);
				} else if (options.isUseCache()) {
					// If the result is not cacheable, see if another thread has cached a value
					MetricResultCacheValue<T> cache = theValueCache;
					if (cache != null) {
						if (cache.supportStamp != theSupportStamp) {
							theValueCache = null;
						} else {
							debug.event("useCachedValue").with("cache", cache::copy).occurred();
							return result.setState(cache);
						}
					}
				}
			}
			debug.event("calculated").with("result", result::copy).with("hasTimeline", true).occurred();
			return result;
		}

		@Override
		public String toString() {
			return holder.toString();
		}
	}

	class MetricResultWrapper<T> implements MetricQueryResult<T> {
		final CachedMetricResult<T> cachedTimeline;
		private Subscription theRemove;
		private boolean isDone;

		MetricResultWrapper(CachedMetricResult<T> timeline, Consumer<Object> updateReceiver) {
			cachedTimeline = timeline;
			theRemove = timeline.notifyOnChange(updateReceiver);
		}

		@Override
		public MetricChannel<T> getChannel() {
			return cachedTimeline.getChannel();
		}

		@Override
		public TypeToken<MetricResult<T>> getType() {
			return cachedTimeline.getType();
		}

		@Override
		public Object getIdentity() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Transaction lock() {
			return cachedTimeline.lock();
		}

		@Override
		public Transaction tryLock() {
			return cachedTimeline.tryLock();
		}

		@Override
		public long getStamp() {
			return cachedTimeline.getStamp();
		}

		@Override
		public boolean isValid() {
			return cachedTimeline.isValid();
		}

		@Override
		public Subscription notifyOnChange(Consumer<Object> onChange) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public MetricResult<T> waitFor(long wait) {
			return cachedTimeline.waitFor(wait);
		}

		@Override
		public void unsubscribe() {
			isDone = true;
			Subscription remove = theRemove;
			if (remove != null) {
				theRemove = null;
				remove.unsubscribe();
			}
		}

		@Override
		public void checkDynamicDependencies() {
			cachedTimeline.checkDynamicDependencies();
		}

		@Override
		public MetricResult<T> getResult(MetricResult<T> result, MetricQueryOptions options) {
			return cachedTimeline.getResult(result, options);
		}

		@Override
		public String toString() {
			return cachedTimeline.toString();
		}
	}

	static class MetricResultCacheValue<T> extends MetricResult<T> {
		final int supportStamp;

		MetricResultCacheValue(int supportStamp, MetricResult<T> toCopy) {
			super(toCopy);
			this.supportStamp = supportStamp;
		}

		@Override
		public MetricResult<T> setValue(T value) {
			throw new IllegalStateException("Immutable");
		}

		@Override
		public MetricResult<T> unavailable(boolean computing) {
			throw new IllegalStateException("Immutable");
		}

		@Override
		public MetricResult<T> invalid() {
			throw new IllegalStateException("Immutable");
		}
	}
}
