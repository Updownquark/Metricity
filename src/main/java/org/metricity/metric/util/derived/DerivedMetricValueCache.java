package org.metricity.metric.util.derived;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.metricity.anchor.Anchor;
import org.metricity.metric.Metric;
import org.metricity.metric.MetricParameterValue;
import org.metricity.metric.MetricType;
import org.metricity.metric.service.MetricChannel;
import org.qommons.StringUtils;
import org.qommons.collect.BetterCollection;
import org.qommons.collect.BetterSortedMap;
import org.qommons.collect.CollectionElement;
import org.qommons.collect.QuickSet;
import org.qommons.collect.QuickSet.QuickMap;
import org.qommons.tree.BetterTreeMap;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class DerivedMetricValueCache<T> {
	public static class CacheKey<T> {
		private QuickMap<String, Object> dependencyValues;
		boolean stored;

		CacheKey(QuickMap<String, Object> keyValues) {
			dependencyValues = keyValues;
		}

		@Override
		public int hashCode() {
			return dependencyValues.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			} else if (!(obj instanceof CacheKey)) {
				return false;
			}
			return dependencyValues.equals(((CacheKey<?>) obj).dependencyValues);
		}

		@Override
		public String toString() {
			return dependencyValues.toString();
		}

		void release() {
			stored = false;
			dependencyValues.release();
		}
	}

	private static final String ANCHOR = "%%ANCHOR%%";
	private static final String METRIC = "%%METRIC%%";
	private static final String METRIC_PARAM = "%%METRICPARAM%%";
	private static final String SUPPORT = "%%SUPPORT%%";
	private static final Object NULL_VALUE = new Object() {
		@Override
		public String toString() {
			return "Null";
		}
	};

	private class CacheKeySet {
		private final QuickSet<String> dependencies;
		final Cache<CacheKey<T>, T> cachedValues;

		CacheKeySet(QuickSet<String> dependencies) {
			this.dependencies = dependencies;
			cachedValues = CacheBuilder.newBuilder()//
					.concurrencyLevel(8)//
					.expireAfterAccess(1000, TimeUnit.MILLISECONDS)//
					.softValues()//
					.removalListener(new RemovalListener<CacheKey<T>, T>() {
						@Override
						public void onRemoval(RemovalNotification<CacheKey<T>, T> arg0) {
							arg0.getKey().release();
						}
					})
					.build();
		}

		CacheKey<T> valueFor(DependencyValueSet<Anchor, T> depends) {
			for (String depend : dependencies) {
				if (depend.equals(ANCHOR) || depend.equals(METRIC) || depend.startsWith(METRIC_PARAM))
					continue;
				else if (depend.startsWith(SUPPORT))
					continue; // If the supplier is just checking support, don't exclude because it's not supported
				else if (!depends.isSupported(depend))
					return null;
			}
			QuickMap<String, Object> keyValues = dependencies.createMap();
			for (int i = 0; i < dependencies.size(); i++) {
				Object value;
				if (dependencies.get(i).equals(ANCHOR)) {
					value = depends.getAnchor();
				} else if (dependencies.get(i).equals(METRIC)) {
					value = depends.getMetric();
				} else if (dependencies.get(i).startsWith(METRIC_PARAM)) {
					String paramName = dependencies.get(i).substring(METRIC_PARAM.length());
					value = depends.getMetric().getParameter(paramName);
				} else if (dependencies.get(i).startsWith(SUPPORT)) {
					String dName = dependencies.get(i).substring(SUPPORT.length());
					value = depends.isSupported(dName);
				} else {
					value = depends.get(dependencies.get(i));
				}
				keyValues.put(i, value);
			}
			return new CacheKey<>(keyValues);
		}
	}

	private final DerivedMetricChannelType<T> theChannel;
	/** The map is keyed by List<String> and valued CacheKeySet<T> */
	private final BetterSortedMap<QuickSet<String>, CacheKeySet> theCache;
	private final QuickSet<String> theIgnoredDependencies;

	public DerivedMetricValueCache(DerivedMetricChannelType<T> channel, QuickSet<String> ignoredDependencies) {
		theChannel = channel;
		theCache = new BetterTreeMap<>(true, QuickSet::compareTo);
		theIgnoredDependencies = ignoredDependencies;
	}

	public T get(DependencyValueSet<Anchor, T> ds) {
		BetterCollection<CacheKeySet> cacheValues = theCache.values();
		// Since elements are never removed from the cache tree and each of these operations are atomic, no locking is needed here
		CollectionElement<CacheKeySet> keySet = cacheValues.getTerminalElement(true);
		while (keySet != null) {
			CacheKeySet cache = keySet.get();
			CacheKey<T> key = cache.valueFor(ds);
			if (key != null) {
				T value = cache.cachedValues.getIfPresent(key);
				key.release();
				if (value != null) {
					return value;
				}
			} // else Means one of the cache key's dependencies is not supported

			keySet = cacheValues.getAdjacentElement(keySet.getElementId(), true);
		}

		// The value is not cached. Need to calculate.
		ProxyDVS dsProxy = new ProxyDVS(ds);
		T value = theChannel.getValue(dsProxy, false);
		if (value == null) {
			value=(T) NULL_VALUE;
		}
		QuickSet<String> dependencyNames = new QuickSet<>(StringUtils.DISTINCT_NUMBER_TOLERANT, dsProxy.getUsedDependencies());

		CacheKeySet correctCache = theCache.computeIfAbsent(dependencyNames, CacheKeySet::new);
		CacheKey<T> cacheValue = correctCache.valueFor(dsProxy);
		if (cacheValue == null) {
			// One of the dependencies is not supported or available yet
			return null;
		}
		T storeValue = value;
		T correctValue;
		try {
			correctValue = correctCache.cachedValues.get(cacheValue, () -> {
				cacheValue.stored = true;
				return storeValue;
			});
		} catch (ExecutionException e) {
			throw new IllegalStateException("Should not happen", e);
		}
		if (!cacheValue.stored) {
			cacheValue.release();
		}
		return correctValue;
	}

	class ProxyDVS implements DependencyValueSet<Anchor, T> {
		private DependencyValueSet<Anchor, T> theSource;
		boolean usedAnchor;
		boolean usedMetric;
		NavigableSet<String> usedMetricParams;
		Metric<T> proxyMetric;
		NavigableSet<String> usedDependencies;
		NavigableSet<String> checkedSupport;

		ProxyDVS(DependencyValueSet<Anchor, T> source) {
			theSource = source;
		}

		List<String> getUsedDependencies() {
			List<String> dependencyNames = new ArrayList<>();
			if (usedAnchor) {
				dependencyNames.add(ANCHOR);
			}
			if (usedMetric) {
				dependencyNames.add(METRIC);
			} else if (usedMetricParams != null) {
				dependencyNames.addAll(usedMetricParams);
			}
			if (usedDependencies != null) {
				dependencyNames.addAll(usedDependencies);
			}
			if (checkedSupport != null) {
				// If the supplier retrieved the value or the diff after checking support,
				// there's no need to use the support as a key element
				if (usedDependencies != null) {
					checkedSupport.removeAll(usedDependencies);
				}
			}
			((ArrayList<String>) dependencyNames).trimToSize();
			return Collections.unmodifiableList(dependencyNames);
		}

		private void usedParam(String parameterName) {
			if (usedMetric) {
				return;
			}
			usedMetricParams = add(usedMetricParams, parameterName, false);
		}

		private Metric<T> createProxyMetric() {
			class ProxyMetric<X> implements Metric<X> {
				@Override
				public MetricType<X> getType() {
					usedMetric = true;
					return (MetricType<X>) theSource.getMetric().getType();
				}

				@Override
				public Map<String, ? extends MetricParameterValue<?>> getParameters() {
					usedMetric = true;
					return theSource.getMetric().getParameters();
				}

				@Override
				public Object getParameter(String parameterName) {
					usedParam(parameterName);
					return Metric.super.getParameter(parameterName);
				}
			}
			return new ProxyMetric<>();
		}

		@Override
		public Anchor getAnchor() {
			usedAnchor = true;
			return theSource.getAnchor();
		}

		@Override
		public Metric<T> getMetric() {
			if (proxyMetric == null) {
				proxyMetric = createProxyMetric();
			}
			return proxyMetric;
		}

		@Override
		public boolean isSupported(String dependencyName) {
			checkedSupport = add(checkedSupport, dependencyName, true);
			return theSource.isSupported(dependencyName);
		}

		@Override
		public <X> MetricChannel<X> getChannel(String dependency) {
			// Attempting to follow everything the supplier cares about out of dependent channels is too complicated
			// We'll just not allow this
			throw new IllegalStateException("Value suppliers for cached metric channels may not access dependent channels");
		}

		@Override
		public <X> X get(String dependency) {
			usedDependencies = add(usedDependencies, dependency, true);
			return theSource.get(dependency);
		}

		private NavigableSet<String> add(NavigableSet<String> dependencies, String dependency, boolean checkIgnore) {
			if (checkIgnore && theIgnoredDependencies.contains(dependency)) {
				return dependencies;
			}
			if (dependencies == null) {
				dependencies = new TreeSet<>();
			}
			dependencies.add(dependency);
			return dependencies;
		}
	}
}
