package org.metricity.metric.util.derived;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

import org.dbug.DBug;
import org.dbug.DBugAnchor;
import org.dbug.DBugAnchorType;
import org.metricity.metric.service.MetricChannel;
import org.metricity.metric.service.MetricChannelService;
import org.metricity.metric.service.MetricQueryOptions;
import org.metricity.metric.service.MetricQueryResult;
import org.metricity.metric.service.MetricResult;
import org.observe.Subscription;
import org.observe.util.TypeTokens;
import org.qommons.ArrayUtils;
import org.qommons.Lockable;
import org.qommons.Transaction;
import org.qommons.ValueHolder;
import org.qommons.collect.ElementId;
import org.qommons.collect.ListenerList;

import com.google.common.reflect.TypeToken;

/**
 * A derived timeline whose set of dependencies is dynamic, based on a static sub-set
 * 
 * @param <AC> The type of the primary timeline
 * @param <T> The type of the dynamic dependencies (if identical)
 * @param <X> The type of the derived metric
 */
public class DynamicDependencyResult<AC, T, X> extends MetricQueryResult.AbstractMQR<X> {
	public interface DynamicDependencySource<AC, T> {
		AC getPrimary();

		int size();

		boolean isAvailable(int index);

		T get(int index);

		MetricChannel<? extends T> getChannel(int index);

		ChannelTemplate<T> getChannelTemplate(int index);

		List<? extends T> values();
	}
	final MetricQueryResult<AC> thePrimaryResults;
	private final MetricChannelService theDepends;
	final Consumer<Object> theUpdates;
	private final ListenerList<Consumer<Object>> theListeners;

	final Function<AC, ? extends Collection<? extends ChannelTemplate<T>>> theDependencyMap;
	private final Function<DynamicDependencySource<AC, T>, ? extends X> theAggregation;

	private PrimarySample thePrimarySample;
	private final ReentrantReadWriteLock theLock;
	private final AtomicLong thePrimarySampleStamp;
	private volatile boolean isInitialized;
	private volatile int thePrimaryUpdateStamp;
	private volatile boolean isUnsubscribed;

	private static final DBugAnchorType<DynamicDependencyResult<?, ?, ?>> DEBUG_TYPE = DBug.declare("metricity",
		(Class<DynamicDependencyResult<?, ?, ?>>) (Class<?>) DynamicDependencyResult.class, b -> b//
			.withStaticField("channel", new TypeToken<MetricChannel<?>>() {}, ddt -> ddt.getChannel())//
			.withEvent("checkSamples", eb -> eb//
				.withEventField("initial", TypeTokens.get().BOOLEAN))//
			.withEvent("primaryQuery", null)//
			.withEvent("primaryValue", eb -> eb//
				.withEventField("primaryResult", new TypeToken<MetricResult<?>>() {}))//
			.withEvent("noSecondaryQuery", eb -> eb//
				.withEventField("reason", TypeTokens.get().STRING))//
			.withEvent("secondaryQuery", eb -> eb//
				.withEventField("secondaryChannel", new TypeToken<MetricChannel<?>>() {}))//
			.withEvent("secondaryResult", eb -> eb//
				.withEventField("primaryValue", TypeTokens.get().OBJECT)//
				.withEventField("rangeIndex", TypeTokens.get().INT)//
				.withEventField("secondaryChannel", new TypeToken<MetricChannel<?>>() {})//
				.withEventField("secondaryResult", new TypeToken<MetricResult<?>>() {}))//
	);
	private final DBugAnchor<DynamicDependencyResult<?, ?, ?>> debug;

	protected DynamicDependencyResult(Function<AC, ? extends Collection<? extends ChannelTemplate<T>>> dependencyMap,
		Function<DynamicDependencySource<AC, T>, ? extends X> aggregation, MetricChannel<X> channel, MetricQueryResult<AC> relationResults,
		MetricChannelService depends, Consumer<Object> initialListener) {
		super(channel);
		thePrimaryResults = relationResults;
		theDepends = depends;
		theUpdates = initialListener;
		theListeners = ListenerList.build().allowReentrant().withFastSize(false).build();

		theDependencyMap = dependencyMap;
		theAggregation = aggregation;

		theLock = new ReentrantReadWriteLock();
		thePrimarySampleStamp = new AtomicLong();

		debug = DEBUG_TYPE.debug(this).build();
		checkSamples(null, true);
	}

	protected void updateRelation(Object cause) {
		thePrimaryUpdateStamp++;
		if (!isInitialized) {
			return;
		}
		if (!thePrimaryResults.isValid()) {
			try (Transaction t = lockSamplesExclusive()) {
				unsubscribe();
			}
			theUpdates.accept(cause);
		} else {
			checkSamples(cause, false);
		}
	}

	private void checkSamples(Object cause, boolean initial) {
		isInitialized = false;
		try (Transaction dt = debug.event("checkSamples").with("initial", initial).begin();
			Transaction sampleLock = lockSamplesExclusive()) {
			boolean reQuery = false;

			// We avoid taking a lock on the primary timeline by doing this optimistically
			// This loop will re-execute if the primary timeline is changed during it
			int updateStamp;
			do {
				updateStamp = thePrimaryUpdateStamp;
				PrimarySample newSample = compareSamples(thePrimarySample);
				if (newSample != thePrimarySample) {
					thePrimarySample = newSample;
					reQuery = true;
				}
				if (newSample != null) {
					for (ChannelRange ch : newSample.channels)
						ch.queryIfNeeded();
				}
			} while (updateStamp != thePrimaryUpdateStamp);
			if (reQuery) {
				thePrimarySampleStamp.getAndIncrement();
				if (cause != null) {
					theUpdates.accept(cause);
					theListeners.forEach(//
						listener -> listener.accept(cause));
				}
			}
		} finally {
			isInitialized = true;
		}
	}

	private PrimarySample compareSamples(PrimarySample oldPrimarySample) {
		MetricResult<AC> primaryResult = thePrimaryResults.getResult(new MetricResult<>(), MetricQueryOptions.get());
		ChannelTemplate<T>[] channels = getChannels(primaryResult, MetricQueryOptions.get());
		ChannelRange[] ranges;
		if (oldPrimarySample == null) {
			ranges = new DynamicDependencyResult.ChannelRange[channels.length];
			for (int i = 0; i < channels.length; i++)
				ranges[i] = new ChannelRange(channels[i]);
			return new PrimarySample(primaryResult, ranges);
		} else {
			boolean[] diff = new boolean[1];
			ranges = ArrayUtils.adjust(oldPrimarySample.channels, channels,
				new ArrayUtils.DifferenceListener<ChannelRange, ChannelTemplate<T>>() {
					@Override
					public boolean identity(ChannelRange o1, ChannelTemplate<T> o2) {
						if (o1 == null)
							return o1 == null;
						else if (o2 == null)
							return false;
						else
							return o1.channel.equals(o2);
					}

					@Override
					public ChannelRange added(ChannelTemplate<T> o, int mIdx, int retIdx) {
						diff[0] = true;
						return new ChannelRange(o);
					}

					@Override
					public ChannelRange removed(ChannelRange o, int oIdx, int incMod, int retIdx) {
						o.unsubscribe();
						return null;
					}

					@Override
					public ChannelRange set(ChannelRange o1, int idx1, int incMod, ChannelTemplate<T> o2, int idx2, int retIdx) {
						return o1;
					}
				});
			if (diff[0])
				return new PrimarySample(primaryResult, ranges);
			else
				return oldPrimarySample;
		}
	}

	ChannelTemplate<T>[] getChannels(MetricResult<AC> primaryResult, MetricQueryOptions options) {
		try (Transaction t = debug.event("primaryQuery").begin()) {
			thePrimaryResults.getResult(primaryResult, options);
			debug.event("primaryValue").with("primaryResult", primaryResult::copy).occurred();
		}
		AC primaryValue = primaryResult.get();
		ChannelTemplate<T>[] channels;
		if (primaryResult.isAvailable()) {
			Collection<? extends ChannelTemplate<T>> newChannels = theDependencyMap.apply(primaryValue);
			if (newChannels == null) {
				channels = (ChannelTemplate<T>[]) EMPTY;
			} else {
				channels = newChannels.toArray(new ChannelTemplate[newChannels.size()]);
			}
		} else {
			channels = (ChannelTemplate<T>[]) EMPTY;
		}
		return channels;
	}

	private static final ChannelTemplate<?>[] EMPTY = new ChannelTemplate[0];

	protected Transaction lockSamples() {
		Lock lock = theLock.readLock();
		lock.lock();
		return lock::unlock;
	}

	private Transaction lockSamplesExclusive() {
		Lock lock = theLock.writeLock();
		lock.lock();
		return lock::unlock;
	}

	@Override
	public Transaction lock() {
		return Lockable.lockAll(Lockable.collapse(Arrays.asList(thePrimaryResults, Lockable.lockable(theLock, false))), //
			() -> thePrimarySample == null ? Collections.emptyList() : Arrays.asList(thePrimarySample.channels), //
			channel -> channel.getResults());
	}

	@Override
	public Transaction tryLock() {
		return Lockable.tryLockAll(Lockable.collapse(Arrays.asList(thePrimaryResults, Lockable.lockable(theLock, false))), //
			() -> thePrimarySample == null ? Collections.emptyList() : Arrays.asList(thePrimarySample.channels), //
			channel -> channel.getResults());
	}

	@Override
	public long getStamp() {
		long stamp = thePrimaryResults.getStamp();
		PrimarySample ps = thePrimarySample;
		if (ps == null)
			return stamp;
		int shift = 64 / (ps.channels.length + 1);
		int i = 1;
		for (ChannelRange range : ps.channels) {
			if (range == null)
				continue;
			long valueStamp = range.getResults().getStamp();
			valueStamp = Long.rotateRight(valueStamp, shift * i);
			stamp ^= valueStamp;
		}
		return stamp;
	}

	@Override
	public boolean isValid() {
		return thePrimaryResults.isValid();
	}

	@Override
	public void unsubscribe() {
		isUnsubscribed = true;
		thePrimaryResults.unsubscribe();
		lockSamplesExclusive().close(); // Let any current update finish
		PrimarySample priSample = thePrimarySample;
		for (ChannelRange chRange : priSample.channels) {
			if (chRange != null) {
				chRange.unsubscribe();
			}
		}
	}

	@Override
	public void checkDynamicDependencies() {
		thePrimaryResults.checkDynamicDependencies();
		try (Transaction t = lockSamples()) {
			PrimarySample ps = thePrimarySample;
			for (ChannelRange chRange : ps.channels) {
				chRange.checkDynamicDependencies();
			}
		}
	}

	@Override
	public MetricResult<X> getResult(MetricResult<X> result, MetricQueryOptions options) {
		PrimarySample primary = thePrimarySample;

		AC primaryValue;
		boolean cacheable = true;
		ChannelRange[] ranges;
		boolean temporaryRanges;
		if (primary != null && primary.primaryValue.isCachable() && options.isUseCache()) {
			if (primary.primaryValue.isAvailable()) {
				primaryValue = primary.primaryValue.get();
				ranges = primary.channels;
				temporaryRanges = false;
			} else {
				return result.setState(primary.primaryValue.reuse());
			}
		} else {
			// The primary value is not cacheable; need to re-evaluate.
			// May be able to use the primary sample's channels as a starting point though.
			temporaryRanges = true;
			ChannelTemplate<T>[] channels;
			if (primary != null) {
				channels = getChannels(result.reuse(), options);
			} else {
				// As noted above, have to handle this as a dependency of itself due to this timeline's required caching
				channels = getChannels(result.reuse(), options);
				if (!result.isAvailable()) {
					return result;
				}
			}
			if (!result.isAvailable()) {
				return result;
			}
			primaryValue = (AC) result.get();
			cacheable &= result.isCachable();
			temporaryRanges = true;
			ranges = new DynamicDependencyResult.ChannelRange[channels.length];
			if (primary != null && primary.channels.length > 0) {
				ArrayUtils.adjustNoCreate(primary.channels, channels,
					new ArrayUtils.DifferenceListener<ChannelRange, ChannelTemplate<T>>() {
						@Override
						public boolean identity(ChannelRange o1, ChannelTemplate<T> o2) {
							return o1.channel != null && o1.channel.equals(o2);
						}

						@Override
						public ChannelRange added(ChannelTemplate<T> o, int mIdx, int retIdx) {
							ranges[retIdx] = new ChannelRange(o);
							ranges[retIdx].query();
							return ranges[retIdx];
						}

						@Override
						public ChannelRange removed(ChannelRange o, int oIdx, int incMod, int retIdx) {
							return null;
						}

						@Override
						public ChannelRange set(ChannelRange o1, int idx1, int incMod, ChannelTemplate<T> o2, int idx2, int retIdx) {
							ranges[retIdx] = o1;
							// Don't create a copy range, since that would unnecessarily create a copy timeline
							if (o1.results != null && !o1.needsQuery()) {
								o1.preserveTimeline = true; // Make sure we don't unsubscribe it at the end of this method
							}
							return o1;
						}
					});
			} else {
				for (int i = 0; i < ranges.length; i++) {
					ranges[i] = new ChannelRange(channels[i]);
					ranges[i].query();
				}
			}
		}
		// boolean debugPrimary = false; // For debugging
		// boolean debugValue = false;
		// if (debugPrimary) {
		// thePrimaryResults.getResult(primary.primarySample, time, (MetricResult<AC>) result);
		// }
		boolean[] available = new boolean[ranges.length];
		Object[] values = new Object[ranges.length];
		for (int i = 0; i < values.length; i++) {
			ChannelRange range = ranges[i];
			MetricQueryResult<? extends T> timeline = range.getResults();
			if (timeline != null) {
				((MetricQueryResult<X>) timeline).getResult(result, options);
				debug.event("secondaryResult").with("primaryValue", primaryValue).with("rangeIndex", i)//
					.with("secondaryChannel", timeline.getChannel()).with("secondaryResult", result::copy).occurred();
				cacheable &= result.isCachable();
				if (result.isAvailable()) {
					values[i] = result.get();
				} else if (range.channel.required) {
					// if (debugValue) {
					// if (range.channel.withDiff) {
					// ((QuantitativeMetricResultTimeline<X, ?>) timeline).getDiffResult(chSample, time, result);
					// } else {
					// ((MetricResultTimeline<X>) timeline).getResult(chSample, time, result);
					// }
					// }
					result.cacheable(cacheable);
					debug.event("secondaryResult").with("primaryValue", primaryValue).with("rangeIndex", i)//
						.with("secondaryChannel", timeline.getChannel()).with("secondaryResult", result::copy).occurred();
					return result;
				}
			} else if (range.channel != null && range.channel.required) {
				// if (debugValue) {
				// range.needsQuery = true;
				// range.query();
				// }
				result.unavailable(false).cacheable(cacheable);
				debug.event("secondaryResult").with("primaryValue", primaryValue).with("rangeIndex", i)//
					.with("secondaryChannel", null).with("secondaryResult", result::copy).occurred();
				return result;
			}
		}
		result.setValue(theAggregation.apply(new DynamicDSImpl(primaryValue, ranges, available, values))).cacheable(cacheable);
		if (temporaryRanges) {
			for (int i = 0; i < ranges.length; i++) {
				if (ranges[i].preserveTimeline) {
					ranges[i].preserveTimeline = false;
				} else {
					ranges[i].unsubscribe();
				}
			}
		}
		return result;
	}

	@Override
	public Subscription notifyOnChange(Consumer<Object> onChange) {
		return theListeners.add(onChange, true)::run;
	}

	public class PrimarySample {
		final MetricResult<AC> primaryValue;
		final ChannelRange[] channels;
		ElementId address;

		PrimarySample(MetricResult<AC> primaryValue, ChannelRange[] channels) {
			this.primaryValue = primaryValue.copy();
			this.channels = channels;
		}

		public int getChannelCount() {
			return channels.length;
		}

		public ChannelRange getChannel(int index) {
			return channels[index];
		}
	}

	public class ChannelRange {
		public final ChannelTemplate<T> channel;
		MetricChannel<? extends T> metricChannel;
		private long theSupportStamp = -1;
		private volatile MetricQueryResult<? extends T> results;
		private volatile boolean isSubscribed;
		private boolean needsQuery;
		private boolean preserveTimeline;

		ChannelRange(ChannelTemplate<T> channel) {
			this.channel = channel;
			if (channel != null) {
				needsQuery = true;
			}
		}

		boolean needsQuery() {
			return needsQuery;
		}

		void queryIfNeeded() {
			if (needsQuery) {
				query();
			}
			preserveTimeline = false;
		}

		boolean checkDynamicDependencies() {
			if (channel == null || channel.metric == null) {
				return false;
			}
			long newStamp = theDepends.getSupport().getStamp();
			if (metricChannel == null || theSupportStamp != newStamp) {
				theSupportStamp = newStamp;
				MetricChannel<?> newChannel;
				ValueHolder<String> reason = new ValueHolder<>();
				if (channel.channelFilter != null) {
					newChannel = channel.channelFilter.selectFrom(theDepends.getChannels(channel.anchor, channel.metric, reason),
						theDepends);
				} else {
					newChannel = theDepends.getChannel(channel.anchor, channel.metric, reason);
				}
				if (newChannel == null) {
					debug.event("noSecondaryQuery").with("reason", "noChannel: " + reason.get()).occurred();
				}
				if (!Objects.equals(metricChannel, newChannel)) {
					query();
					return true;
				} else if (results != null) {
					results.checkDynamicDependencies();
				}
			} else if (results != null) {
				results.checkDynamicDependencies();
			}
			return false;
		}

		void query() {
			needsQuery = false;
			if (channel == null || channel.metric == null) {
				debug.event("noSecondaryQuery").with("reason", "nullTemplate").occurred();
				unsubscribe();
				return;
			}

			long newStamp = theDepends.getSupport().getStamp();
			if (metricChannel == null || theSupportStamp != newStamp) {
				theSupportStamp = newStamp;
				ValueHolder<String> reason = new ValueHolder<>();
				if (channel.channelFilter != null) {
					metricChannel = channel.channelFilter.selectFrom(theDepends.getChannels(channel.anchor, channel.metric, reason),
						theDepends);
				} else {
					metricChannel = theDepends.getChannel(channel.anchor, channel.metric, reason);
				}
				if (results != null && !results.getChannel().equals(metricChannel)) {
					unsubscribe();
				}
				if (metricChannel == null) {
					debug.event("noSecondaryQuery").with("reason", "noChannel: " + reason.get()).occurred();
					return;
				}
			}

			MetricQueryResult<? extends T> result;
			try (Transaction dt = debug.event("secondaryQuery").with("secondaryChannel", metricChannel).begin()) {
				if (theUpdates != null) {
					InnerUpdates updates = new InnerUpdates();
					updates.timeline = theDepends.query((MetricChannel<T>) metricChannel, updates);
					result = updates.timeline;
				} else {
					result = theDepends.query((MetricChannel<T>) metricChannel, null);
				}
			}
			if (isUnsubscribed) {
				result.unsubscribe();
			} else {
				isSubscribed = true;
			}
			preserveTimeline = false;
			results = result;
		}

		void unsubscribe() {
			if (results != null && isSubscribed) {
				if (!preserveTimeline) {
					results.unsubscribe();
				}
				isSubscribed = false;
			}
		}

		public MetricQueryResult<? extends T> getResults() {
			if (theSupportStamp != theDepends.getSupport().getStamp()) {
				needsQuery = true;
			}
			if (needsQuery()) {
				query();
			}
			return results;
		}

		@Override
		public int hashCode() {
			return channel.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			} else if (!(obj instanceof DynamicDependencyResult.ChannelRange)) {
				return false;
			}
			ChannelRange other = (ChannelRange) obj;
			return channel.equals(other.channel);
		}

		@Override
		public String toString() {
			return channel.toString();
		}

		class InnerUpdates implements Consumer<Object> {
			volatile MetricQueryResult<? extends T> timeline;

			@Override
			public void accept(Object cause) {
				theUpdates.accept(cause);
			}
		}
	}

	class DynamicDSImpl implements DynamicDependencySource<AC, T> {
		private final AC thePrimaryValue;
		private final ChannelRange[] theRanges;
		private final boolean[] available;
		private final Object[] theValues;

		DynamicDSImpl(AC primaryValue, ChannelRange[] ranges, boolean[] available, Object[] values) {
			thePrimaryValue = primaryValue;
			this.theRanges = ranges;
			this.available = available;
			theValues = values;
		}

		@Override
		public AC getPrimary() {
			return thePrimaryValue;
		}

		@Override
		public int size() {
			return theValues.length;
		}

		@Override
		public boolean isAvailable(int index) {
			return available[index];
		}

		@Override
		public T get(int index) {
			if (index < 0 || index >= theValues.length) {
				throw new IndexOutOfBoundsException(index + " of " + theValues.length);
			}
			return (T) theValues[index];
		}

		@Override
		public MetricChannel<? extends T> getChannel(int index) {
			MetricQueryResult<? extends T> results = theRanges[index].getResults();
			return results == null ? null : results.getChannel();
		}

		@Override
		public ChannelTemplate<T> getChannelTemplate(int index) {
			return theRanges[index].channel;
		}

		@Override
		public List<? extends T> values() {
			return Collections.unmodifiableList((List<T>) Arrays.asList(theValues));
		}
	}
}
