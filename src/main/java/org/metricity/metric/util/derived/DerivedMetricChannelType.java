package org.metricity.metric.util.derived;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.metricity.anchor.Anchor;
import org.metricity.anchor.AnchorSource;
import org.metricity.anchor.AnchorType;
import org.metricity.metric.Metric;
import org.metricity.metric.MetricSupport;
import org.metricity.metric.MetricType;
import org.metricity.metric.SimpleMetric;
import org.metricity.metric.SimpleMetricType;
import org.metricity.metric.service.MetricChannel;
import org.metricity.metric.service.MetricChannelService;
import org.metricity.metric.service.MetricTag;
import org.metricity.metric.util.MetricTagImpl;
import org.qommons.LambdaUtils;
import org.qommons.StringUtils;
import org.qommons.collect.QuickSet;
import org.qommons.collect.QuickSet.QuickMap;

/**
 * A satisfier for a channel defined by {@link org.metricity.metric.util.HelperMetricServiceComponent.ForMetric#derive(Function)}
 * 
 * @param <T> The type of the derived metric
 */
public class DerivedMetricChannelType<T> extends AbstractDerivedChannelType<T> {
	private final int theAdaptiveIndex;
	private final QuickMap<String, MetricDependencyType<T, ?>> theDependencies;
	private final QuickSet<String> theStaticDependencies;
	private final QuickSet<String> theDynamicDependencies;
	private final Function<DependencyValueSet<Anchor, T>, T> theValue;

	private final QuickSet<String> theCacheIgnoredDependencies;
	private final DerivedMetricValueCache<T> theGlobalCache;
	private final boolean useChannelCache;
	private final Function<DependencyValueSet<Anchor, T>, AdaptiveSearchSpace<Object, T>> theSearchSpace;

	DerivedMetricChannelType(SimpleMetricType<T> metric, Predicate<AnchorSource> anchorSource, Predicate<AnchorType<?>> anchorType,
		Function<Anchor, String> anchorFilter, Function<? super SimpleMetric<T>, String> metricFilter, Set<MetricTag> tags,
		Collection<MetricDependencyType<T, ?>> dependencies, double derivationCost, //
		CacheLevel cacheLevel, QuickSet<String> cacheIgnoreDependencies, //
		Function<DependencyValueSet<Anchor, T>, T> value,
		Function<DependencyValueSet<Anchor, T>, AdaptiveSearchSpace<Object, T>> searchSpace) {
		super(metric, anchorSource, anchorType, anchorFilter, metricFilter, tags, derivationCost);
		List<String> allDependNames = new ArrayList<>(dependencies.size());
		LinkedHashSet<String> staticDependNames = null;
		LinkedHashSet<String> dynamicDependNames = null;
		for (MetricDependencyType<T, ?> depend : dependencies) {
			if (depend.isDynamic()) {
				if (dynamicDependNames == null) {
					staticDependNames = new LinkedHashSet<>(dependencies.size() * 3 / 2);
					staticDependNames.addAll(allDependNames);
					dynamicDependNames = new LinkedHashSet<>(dependencies.size());
				}
				dynamicDependNames.add(depend.getName());
			} else if (staticDependNames != null) {
				staticDependNames.add(depend.getName());
			}
			allDependNames.add(depend.getName());
		}
		QuickMap<String, MetricDependencyType<T, ?>> depends = new QuickSet<>(StringUtils.DISTINCT_NUMBER_TOLERANT, allDependNames)
			.createMap();
		for (MetricDependencyType<T, ?> d : dependencies) {
			depends.put(d.getName(), d);

		}
		theDependencies = depends.unmodifiable();
		theAdaptiveIndex = theDependencies.keySet().indexOf(AdaptiveMetricChannel.ADAPTIVE_DEPENDENCY_NAME);
		theStaticDependencies = staticDependNames == null ? theDependencies.keySet()
			: new QuickSet<>(StringUtils.DISTINCT_NUMBER_TOLERANT, staticDependNames);
		theDynamicDependencies = dynamicDependNames == null ? QuickSet.empty()
			: new QuickSet<>(StringUtils.DISTINCT_NUMBER_TOLERANT, dynamicDependNames);
		theValue = value;
		theSearchSpace = searchSpace;

		theCacheIgnoredDependencies = cacheIgnoreDependencies == null ? QuickSet.empty() : cacheIgnoreDependencies;
		if (theCacheIgnoredDependencies != null && !theDependencies.keySet().containsAll(theCacheIgnoredDependencies)) {
			throw new IllegalStateException("One or more cache-ignored dependencies are not defined: " + theCacheIgnoredDependencies);
		}
		if (cacheLevel != null) {
			if (cacheLevel == CacheLevel.TYPE) {
				theGlobalCache = new DerivedMetricValueCache<>(this, theCacheIgnoredDependencies);
				useChannelCache = false;
			} else {
				theGlobalCache = null;
				useChannelCache = true;
			}
		} else {
			theGlobalCache = null;
			useChannelCache = false;
		}
	}

	public boolean useGlobalCache() {
		return theGlobalCache != null;
	}

	public boolean useChannelCache() {
		return useChannelCache;
	}

	public QuickSet<String> getCacheIgnoredDependencies() {
		return theCacheIgnoredDependencies;
	}

	protected Function<DependencyValueSet<Anchor, T>, T> getValue() {
		return theValue;
	}

	public QuickMap<String, MetricDependencyType<T, ?>> getDependencies() {
		return theDependencies;
	}

	public QuickSet<String> getStaticDependencies() {
		return theStaticDependencies;
	}

	public QuickSet<String> getDynamicDependencies() {
		return theDynamicDependencies;
	}

	public int getAdaptiveIndex() {
		return theAdaptiveIndex;
	}

	@Override
	public String isSatisfied(AnchorSource anchorSource, AnchorType anchorType, MetricSupport support) {
		String err = applies(anchorSource, anchorType);
		if (err != null) {
			return err;
		}
		if (support == null) {
			return null; // This method may be called when support is unknown to just test against the anchor
		}
		Set<String> supportedDependencies = new HashSet<>(theDependencies.keySet().size());
		DependencyTypeSet ds = dependencyName -> {
			if (supportedDependencies.contains(dependencyName)) {
				return true;
			} else if (!theDependencies.keySet().contains(dependencyName)) {
				throw new IllegalArgumentException("No such dependency named " + dependencyName);
			} else {
				return false;
			}
		};
		for (MetricDependencyType<T, ?> dependency : theDependencies.allValues()) {
			MetricType<?> depMetricType = dependency.getMetric();
			if (depMetricType == null) {
				continue; // Can't even get the metric type statically
			}
			String msg = support.isSupported(anchorSource, dependency.getAnchorType(anchorType), depMetricType);
			if (msg != null && !dependency.isAdaptive() && dependency.isRequired(ds) == DependencyType.Required) {
				return "Required dependency " + dependency.getName() + " is not satisfied: " + msg;
			}
			if (msg == null) {
				supportedDependencies.add(dependency.getName());
			}
		}
		return null;
	}

	public T getValue(DependencyValueSet<Anchor, T> dependencyValues, boolean useCache) {
		if (useCache && theGlobalCache != null) {
			return theGlobalCache.get(dependencyValues);
		}
		return theValue.apply(dependencyValues);
	}

	@Override
	public MetricChannel<T> createChannel(Anchor anchor, Metric<T> metric, MetricChannelService depends, Consumer<String> onError) {
		String err = applies(anchor, metric);
		if (err != null) {
			if (onError != null) {
				onError.accept(err);
			}
			return null;
		}
		if (theAdaptiveIndex >= 0) {
			return AdaptiveMetricChannel.create(this, anchor, metric, theSearchSpace, depends, onError);
		} else {
			return DerivedMetricChannel.create(this, anchor, metric, depends, onError);
		}
	}

	public static <A extends Anchor, T> Builder<A, T> build(SimpleMetricType<T> metric, //
		Predicate<AnchorSource> anchorSource, Predicate<AnchorType<?>> anchorType, Function<? super SimpleMetric<T>, String> metricFilter) {
		return new Builder<>(metric, anchorSource, anchorType, metricFilter);
	}

	enum CacheLevel {
		TYPE, CHANNEL
	}

	/**
	 * Defines dependencies and derivation for a metric whose value is derived from that of other metrics
	 * 
	 * @author abutler
	 *
	 * @param <A> The type of the target anchor
	 * @param <T> The type of the derived metric
	 */
	public static class Builder<A extends Anchor, T> {
		private final SimpleMetricType<T> theMetric;
		private final Predicate<AnchorSource> theAnchorSource;
		private final Predicate<AnchorType<?>> theAnchorType;
		private Function<A, String> theAnchorFilter;
		private final Function<? super SimpleMetric<T>, String> theMetricFilter;
		private Set<MetricTag> theTags;
		private final Map<String, MetricDependencyType<T, ?>> theDependencies;
		private CacheLevel theCacheLevel;
		private QuickSet<String> theCacheIgnoredDependencies;

		Builder(SimpleMetricType<T> metric, Predicate<AnchorSource> anchorSource, Predicate<AnchorType<?>> anchorType,
			Function<? super SimpleMetric<T>, String> metricFilter) {
			theMetric = metric;
			theAnchorSource = anchorSource;
			theAnchorType = anchorType;
			theMetricFilter = metricFilter;
			theDependencies = new LinkedHashMap<>();
		}

		/** @return The derived metric */
		public SimpleMetricType<T> getMetric() {
			return theMetric;
		}

		/** @return The anchor source test */
		protected Predicate<AnchorSource> getAnchorSource() {
			return theAnchorSource;
		}

		/** @return The anchor type test */
		protected Predicate<AnchorType<?>> getAnchorType() {
			return theAnchorType;
		}

		/** @return The anchor instance test */
		protected Function<A, String> getAnchorFilter() {
			return theAnchorFilter;
		}

		/** @return The metric instance test */
		protected Function<? super SimpleMetric<T>, String> getMetricFilter() {
			return theMetricFilter;
		}

		/** @return The tag set */
		protected Set<MetricTag> getTags() {
			return theTags;
		}

		/** @return The defined dependencies */
		protected Map<String, MetricDependencyType<T, ?>> getDependencies() {
			return theDependencies;
		}

		/** @return The specified cache level */
		protected CacheLevel getCacheLevel() {
			return theCacheLevel;
		}

		/** @return The dependencies ignored for cache values */
		protected QuickSet getCacheIgnoreDependencies() {
			return theCacheIgnoredDependencies;
		}

		/**
		 * @param filter The filter for anchor instances. Returns null if this satisfier can satisfy its metric for the anchor, or a message
		 *        why not (for debugging)
		 * @return This builder
		 */
		public Builder<A, T> filter(Function<A, String> filter) {
			if (theAnchorFilter != null) {
				theAnchorFilter = filter;
			} else {
				Function<A, String> oldFilter = theAnchorFilter;
				theAnchorFilter = LambdaUtils.printableFn(a -> {
					String err = oldFilter.apply(a);
					if (err == null) {
						err = filter.apply(a);
					}
					return err;
				}, () -> oldFilter + " and " + filter);
			}
			return this;
		}

		/**
		 * @param category The category for the tag
		 * @param label The label (value) for the tag
		 * @return This builder
		 */
		public Builder<A, T> withTag(String category, String label) {
			if (theTags == null) {
				theTags = new TreeSet<>();
			}
			theTags.add(new MetricTagImpl(category, label));
			return this;
		}

		/**
		 * Specifies that values produced by this satisfier should be cached by their dependency values to avoid excessive recomputation
		 * 
		 * @param global Whether to cache the values globally, for all channels, or per channel instance
		 * @param ignoredDependencies The names of dependencies that should not be used in the cache key
		 * @return This builder
		 */
		public Builder<A, T> cached(boolean global, String... ignoredDependencies) {
			theCacheLevel = global ? CacheLevel.TYPE : CacheLevel.CHANNEL;
			theCacheIgnoredDependencies = new QuickSet<>(StringUtils.DISTINCT_NUMBER_TOLERANT, ignoredDependencies);
			return this;
		}

		// Dependencies
		/**
		 * Defines a static dependency on another anchor and metric, reachable statically and immutably from the target anchor/metric
		 * 
		 * @param dependencyName The name for the dependency
		 * @param dependency Defines the dependency
		 * @return This builder
		 */
		public Builder<A, T> depends(String dependencyName,
			Function<MetricDependencyType.AnchorDefiner<A, T>, ? extends MetricDependencyType<T, ?>> dependency) {
			if (theDependencies.containsKey(dependencyName)) {
				throw new IllegalArgumentException(theMetric + ": Cannot define 2 dependencies named " + dependencyName);
			}
			MetricDependencyType.AnchorDefiner<A, T> firstStep = new MetricDependencyType.AnchorDefiner<>(dependencyName, false);
			MetricDependencyType<T, ?> dt = dependency.apply(firstStep);
			theDependencies.put(dependencyName, dt);
			return this;
		}

		/**
		 * Defines a dynamic dependency whose anchor, metric, and {@link MetricDependency#isRequired() requirement} may depend on the
		 * resolved target anchor/metric and/or one or more statically-defined dependencies.
		 * 
		 * @param dependencyName The name for the dependency
		 * @param dependency Defines the dependency
		 * @return This builder
		 */
		public Builder<A, T> dependsDynamic(String dependencyName,
			Function<MetricDependencyType.DynamicAnchorDefiner<A, T>, MetricDependencyType<T, ?>> dependency) {
			if (theDependencies.containsKey(dependencyName)) {
				throw new IllegalArgumentException(theMetric + ": Cannot define 2 dependencies named " + dependencyName);
			}
			MetricDependencyType.DynamicAnchorDefiner<A, T> firstStep = new MetricDependencyType.DynamicAnchorDefiner<>(dependencyName);
			MetricDependencyType<T, ?> dt = dependency.apply(firstStep);
			theDependencies.put(dependencyName, dt);
			return this;
		}

		/**
		 * Defines the derivation operation
		 * 
		 * @param derivationCost The notional cost of the derivation operation
		 * @param value The function to produce a target metric value from the dependency value set with all required dependencies present
		 * @return The derived channel type
		 */
		public DerivedMetricChannelType<T> build(double derivationCost, Function<DependencyValueSet<A, T>, T> value) {
			return new DerivedMetricChannelType<>(theMetric, theAnchorSource, theAnchorType,
				(Function<Anchor, String>) (Function<?, String>) theAnchorFilter, theMetricFilter, getTags(), getDependencies().values(),
				derivationCost, //
				getCacheLevel(), getCacheIgnoreDependencies(), //
				(Function<DependencyValueSet<Anchor, T>, T>) (Function<?, ?>) value, null);
		}

		/**
		 * Produces an adaptive derived metric. An adaptive metric is one that may create a cyclical dependency with itself and uses
		 * repeated queries to resolve an optimal value.
		 * 
		 * @param <X> The the type of the adaptive dependency metric
		 * @param derivationCost The notional cost of the adaptation operation
		 * @param dependency Defines the adaptive dependency, which may have the target anchor/metric as a dependency
		 * @param searchSpace Creates an adaptive search space to adapt the target metric value using the adaptive and other dependencies
		 * @return The derived channel type
		 */
		public <X> DerivedMetricChannelType<T> adapt(double derivationCost,
			Function<MetricDependencyType.AnchorDefiner<A, T>, MetricDependencyType<T, X>> dependency, //
			Function<DependencyValueSet<A, T>, AdaptiveSearchSpace<X, T>> searchSpace) {
			if (theCacheLevel != null) {
				throw new IllegalStateException("Adaptive channels may do not support caching");
			}
			for (MetricDependencyType<T, ?> dep : theDependencies.values()) {
				if (dep.isDynamic()) {
					throw new IllegalStateException("Adaptive channels may do not support dynamic dependencies");
				}
			}
			String dependencyName = AdaptiveMetricChannel.ADAPTIVE_DEPENDENCY_NAME;
			MetricDependencyType.AnchorDefiner<A, T> firstStep = new MetricDependencyType.AnchorDefiner<>(dependencyName, true);
			MetricDependencyType<T, ?> dt = dependency.apply(firstStep);
			theDependencies.put(dependencyName, dt);
			return new DerivedMetricChannelType<>(theMetric, theAnchorSource, theAnchorType,
				(Function<Anchor, String>) (Function<?, String>) theAnchorFilter, theMetricFilter, getTags(), getDependencies().values(),
				derivationCost, //
				getCacheLevel(), getCacheIgnoreDependencies(), //
				null, (Function<DependencyValueSet<Anchor, T>, AdaptiveSearchSpace<Object, T>>) (Function<?, ?>) searchSpace);
		}
	}
}
