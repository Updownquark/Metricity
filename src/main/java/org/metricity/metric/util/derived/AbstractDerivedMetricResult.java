package org.metricity.metric.util.derived;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;

import org.metricity.anchor.Anchor;
import org.metricity.metric.Metric;
import org.metricity.metric.service.MetricChannel;
import org.metricity.metric.service.MetricQueryOptions;
import org.metricity.metric.service.MetricQueryResult;
import org.metricity.metric.service.MetricQueryService;
import org.metricity.metric.service.MetricResult;
import org.qommons.collect.QuickSet.QuickMap;

public abstract class AbstractDerivedMetricResult<T> extends CombinedMetricResults<T> {
	private final QuickMap<String, MetricDependency<T, ?>> theDependencies;
	private final MetricQueryResult<?>[] theDependencyResults;
	private volatile boolean isValid;

	volatile boolean isInitialized;

	public AbstractDerivedMetricResult(MetricChannel<T> channel, QuickMap<String, MetricDependency<T, ?>> dependencies) {
		super(channel, dependencies.keySet().size());
		theDependencies = dependencies;
		isValid = true;

		theDependencyResults = new MetricQueryResult[theDependencies.keySet().size()];
		for (int i = 0; i < theDependencies.keySet().size(); i++) {
			MetricDependency<T, ?> dc = theDependencies.get(i);
			if (dc == null || dc.isDynamic()) {
				continue;
			}
		}
	}

	protected void queryDependencies(Consumer<Object> initialListener, MetricQueryService service) {
		isInitialized = false;
		for (int i = 0; i < theDependencyResults.length; i++) {
			MetricDependency<T, ?> dep = theDependencies.get(i);
			if (dep == null || dep.isDynamic()) {
				continue;
			}
			queryDependency(i, dep, service, initialListener);
		}
		isInitialized = true;
	}

	private void queryDependency(int index, MetricDependency<T, ?> dep, MetricQueryService service, Consumer<Object> initialListener) {
		class DependencyUpdater implements Consumer<Object> {
			MetricQueryResult<?> result;

			@Override
			public void accept(Object update) {
				if (!isInitialized || result != theDependencyResults[index]) {
					// Not all the channel result sets may have been obtained, so we can't do anything
					// But that's fine because the initial results should include the updated values
					return;
				}
				update(index, dep.isRequired(), update, initialListener);
			}
		}
		DependencyUpdater depUpdate = new DependencyUpdater();
		depUpdate.result = service.query(dep.getChannel(), depUpdate);
		theDependencyResults[index] = depUpdate.result;
	}

	@Override
	protected MetricQueryResult<?> getDependency(int dependency) {
		return theDependencyResults[dependency];
	}

	@Override
	protected boolean isRequired(int dependency) {
		MetricDependency<T, ?> dep = theDependencies.get(dependency);
		return dep != null && !dep.isDynamic() && dep.isRequired();
	}

	protected ParamMapMDS<T> createDS(MetricResult<T> result, MetricQueryOptions options) {
		IntFunction<MetricResult<?>> valueSupplier = channelIdx -> {
			MetricQueryResult<?> timeline = getDependency(channelIdx);
			if (timeline == null) {
				return result.unavailable(false);
			}
			timeline.getResult(result.reuse(), options);
			return result;
		};
		IntPredicate support = channelIdx -> {
			return getDependency(channelIdx) != null;
		};
		AbstractDerivedMetricResult.ParamMapMDS<T> mds = new AbstractDerivedMetricResult.ParamMapMDS<>(getChannel(), theDependencies,
			valueSupplier, support);
		return mds;
	}

	protected void invalidate(Consumer<Object> updateReceiver, Object cause) {
		isValid = false;
		if (updateReceiver != null)
			updateReceiver.accept(cause);
	}

	protected void update(int dependency, boolean required, Object cause, Consumer<Object> initialListener) {
		MetricQueryResult<?> dep = getDependency(dependency);
		if (dep != null && !dep.isValid()) {
			invalidate(initialListener, cause);
		} else
			initialListener.accept(cause);
	}

	@Override
	public boolean isValid() {
		return isValid;
	}

	protected boolean checkAvailable(ParamMapMDS<T> mds, MetricResult<T> result, MetricQueryOptions options) {
		// First check the status and pre-populate required values
		for (int i = 0; i < getDependencyCount(); i++) {
			MetricDependency<T, ?> dep = theDependencies.get(i);
			if (dep == null || dep.getType().isAdaptive() || dep.isDynamic()) {
				continue;
			}
			MetricQueryResult<?> qRes = getDependency(i);
			if (dep.isRequired() && qRes == null) {
				result.unavailable(false);
				return false;
			} else {
				mds.getResult(qRes, i, result, options);
				if (!result.isAvailable()) {
					return false;
				}
			}
		}
		// If all the required values are available, then the derived value is also available, regardless of optional result
		// availability
		return true;
	}
	public static class ParamMapMDS<T> implements DependencyValueSet<Anchor, T> {
		private final MetricChannel<T> theChannel;
		private final QuickMap<String, MetricDependency<T, ?>> theDependencies;
		private final IntPredicate isSupported;
		private final IntFunction<? extends MetricResult<?>> theValueSupplier;
		private final Object[] theCache;
		private boolean isCacheable;

		public ParamMapMDS(MetricChannel<T> channel, QuickMap<String, MetricDependency<T, ?>> dependencies,
			IntFunction<MetricResult<?>> valueSupplier, IntPredicate support) {
			theChannel = channel;
			theDependencies = dependencies;
			isSupported = support;
			theValueSupplier = valueSupplier;
			theCache = new Object[theDependencies.keySet().size()];
			isCacheable = true;
		}

		@Override
		public Anchor getAnchor() {
			return theChannel.getAnchor();
		}

		@Override
		public Metric<T> getMetric() {
			return theChannel.getMetric();
		}

		@Override
		public <X> MetricChannel<X> getChannel(String dependency) {
			MetricDependency<T, X> dep = (MetricDependency<T, X>) theDependencies.get(dependency);
			// TODO Is this right for dynamic?
			return dep == null ? null : dep.getChannel();
		}

		public void getResult(MetricQueryResult<?> queryResults, int channelIdx, MetricResult<?> result, MetricQueryOptions options) {
			MetricDependency<T, Object> dep = (MetricDependency<T, Object>) theDependencies.get(channelIdx);
			if (dep == null || queryResults == null) {
				result.unavailable(false);
			} else {
				((MetricQueryResult<Object>) queryResults).getResult(result.reuse(), options);
			}
			if (result.isAvailable()) {
				isCacheable &= result.isCachable();
				theCache[channelIdx] = result.get();
			}
		}

		@Override
		public <X> X get(String channel) {
			int keyIdx = theDependencies.keyIndex(channel);
			if (theCache[keyIdx] == null) {
				MetricResult<?> r = theValueSupplier.apply(keyIdx);
				theCache[keyIdx] = r.get();
				isCacheable &= r.isCachable();
			}
			return (X) theCache[keyIdx];
		}

		public void clearCache() {
			isCacheable = true;
			Arrays.fill(theCache, null);
		}

		@Override
		public boolean isSupported(String channel) {
			int keyIdx = theDependencies.keyIndex(channel);
			return theCache[keyIdx] != null//
				|| isSupported.test(keyIdx);
		}

		public boolean isCacheable() {
			return isCacheable;
		}

		@Override
		public String toString() {
			return theChannel.toString();
		}
	}
}
