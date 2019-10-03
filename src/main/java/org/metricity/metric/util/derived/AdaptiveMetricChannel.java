package org.metricity.metric.util.derived;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.dbug.DBug;
import org.dbug.DBugAnchor;
import org.dbug.DBugAnchorType;
import org.metricity.anchor.Anchor;
import org.metricity.metric.Metric;
import org.metricity.metric.service.MetricChannel;
import org.metricity.metric.service.MetricChannelService;
import org.metricity.metric.service.MetricQueryOptions;
import org.metricity.metric.service.MetricQueryResult;
import org.metricity.metric.service.MetricQueryService;
import org.metricity.metric.service.MetricResult;
import org.metricity.metric.service.MetricTag;
import org.metricity.metric.service.TaggedMetricChannel;
import org.metricity.metric.util.SmartMetricChannel;
import org.metricity.metric.util.derived.DerivedMetricChannel.DependencyInstanceSetImpl;
import org.observe.util.TypeTokens;
import org.qommons.BiTuple;
import org.qommons.Transaction;
import org.qommons.collect.QuickSet.QuickMap;

import com.google.common.reflect.TypeToken;

public class AdaptiveMetricChannel<X, T> implements SmartMetricChannel<T>, TaggedMetricChannel<T> {
	public static final String ADAPTIVE_DEPENDENCY_NAME = "$adaptive$";
	private static final ThreadLocal<Map<BiTuple<Anchor, Metric<?>>, AdaptiveMetricChannel<?, ?>>> theCreateCycleBreaker = new ThreadLocal<>();
	private static final int RANGE = 1;
	private static final int COST = 2;
	private static final int PROPERTIES = 4;
	private static final int EQUALS = 8;

	private final DerivedMetricChannelType<T> theType;
	private final Anchor theAnchor;
	private final Metric<T> theMetric;
	private final ThreadLocal<Integer> theChannelState;
	private final QuickMap<String, MetricDependency<T, ?>> theDependencies;
	private final Function<DependencyValueSet<Anchor, T>, AdaptiveSearchSpace<X, T>> theSearchSpace;

	private final int hashCode;

	public AdaptiveMetricChannel(DerivedMetricChannelType<T> type, Anchor anchor, Metric<T> metric,
		QuickMap<String, MetricDependency<T, ?>> dependencies,
		Function<DependencyValueSet<Anchor, T>, AdaptiveSearchSpace<X, T>> searchSpace) {
		theType = type;
		theAnchor = anchor;
		theMetric = metric;
		theChannelState = ThreadLocal.withInitial(() -> 0);
		theDependencies = dependencies.unmodifiable();
		theSearchSpace = searchSpace;
		ensureHasRequired(dependencies);

		hashCode = Objects.hash(theAnchor, theMetric, theDependencies);
	}

	private void ensureHasRequired(QuickMap<String, MetricDependency<T, ?>> dependencies) {
		boolean hasRequired = false;
		for (int i = 0; i < dependencies.keySet().size(); i++) {
			MetricDependency<T, ?> dep = dependencies.get(i);
			if (dep != null && dep.isRequired()) {
				hasRequired = true;
				break;
			}
		}
		if (!hasRequired) {
			throw new IllegalStateException(this + ": At least one required dependency must be defined");
		}
	}

	@Override
	public Anchor getAnchor() {
		return theAnchor;
	}

	@Override
	public Metric<T> getMetric() {
		return theMetric;
	}

	@Override
	public boolean isRecursive() {
		return true;
	}

	@Override
	public Set<MetricTag> getTags() {
		return theType.getTags();
	}

	public DerivedMetricChannelType<T> getType() {
		return theType;
	}

	public QuickMap<String, MetricDependency<T, ?>> getDependencies() {
		return theDependencies;
	}

	@Override
	public double getCost(MetricChannelService depends) {
		// Due to the recursive nature of this channel, we have to safeguard against infinite recursion
		int state = theChannelState.get();
		if ((state & COST) != 0) {
			return 0;
		}
		theChannelState.set(state | COST);
		try {
			double c = theType.getDerivationCost();
			for (MetricDependency<T, ?> dep : theDependencies.values()) {
				if (dep.getType().isAdaptive()) {
					continue;
				}
				MetricChannel<?> depChannel;
				depChannel = dep.getChannel();
				c += depends.getCost(depChannel);
			}
			return c;
		} finally {
			theChannelState.set(state);
		}
	}

	@Override
	public Set<MetricTag> getTags(MetricChannelService depends) {
		return theType.getTags();
	}

	@Override
	public boolean isConstant(MetricChannelService depends) {
		// Due to the recursive nature of this channel, we have to safeguard against infinite recursion
		int state = theChannelState.get();
		if ((state & PROPERTIES) != 0) {
			return false;
		}
		theChannelState.set(state | PROPERTIES);
		try {
			for (MetricDependency<T, ?> dep : theDependencies.values()) {
				if (!depends.isConstant(dep.getChannel()))
					return false;
			}
			return true;
		} finally {
			theChannelState.set(state);
		}
	}

	@Override
	public MetricQueryResult<T> query(MetricChannelService depends, Consumer<Object> initialListener) {
		return new AdaptiveMetricResult<>(this, initialListener, depends);
	}

	public AdaptiveSearchSpace<X, T> createSearchSpace(DependencyValueSet<Anchor, T> values) {
		return theSearchSpace.apply(values);
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (!(obj instanceof AdaptiveMetricChannel)) {
			return false;
		}
		// Due to the recursive nature of this channel, we have to safeguard against infinite recursion
		int state = theChannelState.get();
		if ((state & EQUALS) != 0) {
			return true;
		}
		theChannelState.set(state | EQUALS);
		try {
			AdaptiveMetricChannel<?, ?> other = (AdaptiveMetricChannel<?, ?>) obj;
			if (hashCode != -1 && other.hashCode != -1 && hashCode != other.hashCode) {
				return false;
			}
			return theType == other.theType//
				&& theAnchor.equals(other.theAnchor)//
				&& theMetric.equals(other.theMetric)//
				&& theDependencies.equals(other.theDependencies);
		} finally {
			theChannelState.set(state);
		}
	}

	@Override
	public String toString() {
		return theAnchor + "." + theMetric;
	}

	public static <X, T> AdaptiveMetricChannel<X, T> create(DerivedMetricChannelType<T> type, Anchor anchor, Metric<T> metric,
		Function<DependencyValueSet<Anchor, T>, AdaptiveSearchSpace<X, T>> searchSpace, MetricChannelService depends,
		Consumer<String> onError) {
		BiTuple<Anchor, Metric<?>> key = new BiTuple<>(anchor, metric);
		Map<BiTuple<Anchor, Metric<?>>, AdaptiveMetricChannel<?, ?>> map = theCreateCycleBreaker.get();
		if (map == null) {
			map = new HashMap<>();
			theCreateCycleBreaker.set(map);
		} else {
			AdaptiveMetricChannel<?, ?> ch = map.get(key);
			if (ch != null) {
				if (map.isEmpty()) {
					theCreateCycleBreaker.remove();
				}
				return (AdaptiveMetricChannel<X, T>) ch;
			}
		}
		QuickMap<String, MetricDependencyType<T, ?>> dependencies = type.getDependencies();
		QuickMap<String, MetricDependency<T, ?>> dcs = dependencies.keySet().createMap();
		DependencyInstanceSetImpl<T> depSet = new DependencyInstanceSetImpl<>(anchor, metric);
		boolean[] failure = new boolean[1];
		for (int i = 0; i < dependencies.keySet().size(); i++) {
			MetricDependencyType<T, ?> dep = dependencies.get(i);
			if (dep.isAdaptive()) {
				continue;
			}
			MetricDependency<T, ?> instance = DerivedMetricChannel.buildDependency(dep, anchor, metric, depSet, depends, failure, onError);
			if (failure[0]) {
				return null;
			} else if (instance != null) {
				depSet.dcNames.add(dep.getName());
				dcs.put(i, instance);
			}
		}
		AdaptiveMetricChannel<X, T> ch = new AdaptiveMetricChannel<X, T>(type, anchor, metric, dcs.unmodifiable(), searchSpace);
		map.put(key, ch);
		int adaptIndex = type.getAdaptiveIndex();
		dcs.put(adaptIndex,
			DerivedMetricChannel.buildDependency(dependencies.get(adaptIndex), anchor, metric, depSet, depends, failure, onError));
		map.remove(key);
		if (failure[0]) {
			return null;
		}
		return ch;
	}

	public static <T> QuickMap<String, MetricDependency<T, ?>> compileDependencyChannelsWithoutAdaptive(
		QuickMap<String, MetricDependencyType<T, ?>> dependencies, Anchor mainAnchor, Metric<T> mainMetric,
		MetricChannelService dependencyService, Consumer<String> onError) {
		QuickMap<String, MetricDependency<T, ?>> dcs = dependencies.keySet().createMap();
		DependencyInstanceSetImpl<T> depSet = new DependencyInstanceSetImpl<>(mainAnchor, mainMetric);
		boolean[] failure = new boolean[1];
		for (int i = 0; i < dependencies.keySet().size(); i++) {
			MetricDependencyType<T, ?> dep = dependencies.get(i);
			if (dep.isAdaptive()) {
				continue;
			}
			MetricDependency<T, ?> instance = DerivedMetricChannel.buildDependency(dep, mainAnchor, mainMetric, depSet, dependencyService,
				failure, onError);
			if (failure[0]) {
				return null;
			} else if (instance != null) {
				depSet.dcNames.add(dep.getName());
				dcs.put(i, instance);
			}
		}
		return dcs;
	}

	public static class AdaptiveMetricResult<X, T> extends AbstractDerivedMetricResult<T> {
		private static final int LOCKED = 1;
		private static final int UPDATING = 2;
		private static final int COMPLETE_CALL = 4;
		private static final int UPDATING_CALL = 8;
		private static final int SAMPLING_CALL = 16;
		private static final int CHECK_DEPENDS_CALL = 32;
		private static final int EQUALS_CALL = 64;
		private static final int HASH_CODE_CALL = 128;

		private final MetricQueryService theService;
		private final ThreadLocal<Integer> theState;
		private final ThreadLocal<AdaptiveSearchSpace<X, T>> theSearchSpace;
		private final int theAdaptiveDependencyIndex;

		public AdaptiveMetricResult(AdaptiveMetricChannel<X, T> channel, Consumer<Object> initialListener, MetricQueryService depends) {
			super(channel, channel.getDependencies());
			theService = depends;
			theState = ThreadLocal.withInitial(() -> 0);
			theSearchSpace = new ThreadLocal<>();
			theAdaptiveDependencyIndex = channel.getType().getAdaptiveIndex();

			queryDependencies(initialListener, depends);
		}

		@Override
		public AdaptiveMetricChannel<X, T> getChannel() {
			return (AdaptiveMetricChannel<X, T>) super.getChannel();
		}

		@Override
		public Transaction lock() {
			// Due to the recursive nature of this timeline, we have to safeguard against infinite recursion
			int state = theState.get();
			if ((state & LOCKED) != 0) {
				return Transaction.NONE;
			}
			state |= LOCKED;
			theState.set(state);
			Transaction superLock = super.lock();
			return () -> {
				superLock.close();
				theState.set(theState.get().intValue() & ~LOCKED);
			};
		}

		@Override
		public void checkDynamicDependencies() {
			// Due to the recursive nature of this timeline, we have to safeguard against infinite recursion
			int state = theState.get();
			if ((state & CHECK_DEPENDS_CALL) != 0) {
				return;
			}
			state |= CHECK_DEPENDS_CALL;
			theState.set(state);
			super.checkDynamicDependencies();
			theState.set(theState.get().intValue() & ~CHECK_DEPENDS_CALL);
		}

		private static final DBugAnchorType<AdaptiveMetricResult<?, ?>> DEBUG_TYPE = DBug.declare("metricity",
			(Class<AdaptiveMetricResult<?, ?>>) (Class<?>) AdaptiveMetricResult.class, b -> b//
				.withStaticField("channel", new TypeToken<MetricChannel<?>>() {}, t -> t.getChannel())//
				.withEvent("adapt", null)//
				.withEvent("adapted",
					eb -> eb.withEventField("rounds", TypeTokens.get().INT).withEventField("result", new TypeToken<MetricResult<?>>() {}))//
				.withEvent("fail", null)//
				.withEvent("intermediate", eb -> eb.withEventField("result", new TypeToken<MetricResult<?>>() {})));
		private final DBugAnchor<AdaptiveMetricResult<?, ?>> debug = DEBUG_TYPE.debug(this).build();

		@Override
		public MetricResult<T> getResult(MetricResult<T> result, MetricQueryOptions options) {
			// This class relies on the caching mechanism in CompositeMetricService
			// If it is ever possible to create this kind of timeline without such a caching mechanism,
			// this class will cause StackOverflowErrors

			AdaptiveSearchSpace<X, T> searchSpace = theSearchSpace.get();
			if (searchSpace != null) {
				// This thread is already calculating the result (in the code below)
				// Just return the value adapted so far
				searchSpace.getCurrentValue(result);
				debug.event("intermediate").with("result", result::copy).occurred();
				if (!searchSpace.isComplete()) {
					result.uncacheable();
				}
				return result;
			}
			try (Transaction t = debug.event("adapt").begin()) {
				ParamMapMDS<T> mds = createDS(result, options);
				searchSpace = getChannel().createSearchSpace(mds);
				if (debug.isActive()) {
					AdaptiveSearchSpace.DEBUG_TYPE.debug(searchSpace).with("channel", getChannel()).build();
				}
				searchSpace.getCurrentValue(result);
				if (searchSpace.isComplete()) {
					debug.event("adapted").with("rounds", 0).with("result", result::copy).occurred();
					return result;
				}

				// Need to adapt
				MetricQueryResult<X> adaptiveResult = (MetricQueryResult<X>) getDependency(theAdaptiveDependencyIndex);
				if (adaptiveResult == null) {
					debug.event("fail").occurred();
					return result.unavailable(false);
				}

				theSearchSpace.set(searchSpace);// Store the search space so that recursive calls simply return the current value (see
												// above)

				boolean cacheable;
				int rounds = 0;
				do {
					adaptiveResult.getResult(result.reuse(), options);
					cacheable = result.isCachable();
					if (result.isAvailable()) {
						searchSpace.adapt(result.reuse());
					}
					rounds++;
				} while (!cacheable && result.isAvailable() && !searchSpace.isComplete());
				theSearchSpace.remove(); // Clear the search space so we can do it again on the next invocation
				debug.event("adapted").with("rounds", rounds).with("result", result::copy).occurred();
				if (!cacheable) {
					result.uncacheable();
				} else if (!searchSpace.isComplete()) {
					// If the result is cacheable, but the search is incomplete, that means that it was not evaluated
					// with this as a dependency
					// Therefore, there's nothing to adapt
					// This may happen if a different thread completes the adaptation and the value for this channel or the one being
					// adapted is cached, in which case we'll instruct the caller to just use the cached value
					result.uncacheable();
				}

				return result;
			}
		}

		@Override
		protected void update(int dependency, boolean required, Object dependencyUpdate, Consumer<Object> updateReceiver) {
			if (!getDependency(dependency).isValid()) {
				invalidate(updateReceiver, dependencyUpdate);
				return;
			}
			// Due to the recursive nature of this timeline, we have to safeguard against infinite recursion
			int state = theState.get();
			if ((state & UPDATING) != 0) {
				return;
			}
			state |= UPDATING;
			theState.set(state);
			super.update(dependency, required, dependencyUpdate, updateReceiver);
			theState.set(theState.get().intValue() & UPDATING);
		}
	}
}
