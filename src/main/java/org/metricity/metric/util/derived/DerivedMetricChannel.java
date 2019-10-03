package org.metricity.metric.util.derived;

import java.time.Instant;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.metricity.anchor.Anchor;
import org.metricity.metric.Metric;
import org.metricity.metric.service.MetricChannel;
import org.metricity.metric.service.MetricChannelService;
import org.metricity.metric.service.MetricQueryOptions;
import org.metricity.metric.service.MetricQueryResult;
import org.metricity.metric.service.MetricQueryService;
import org.metricity.metric.service.MetricResult;
import org.qommons.IterableUtils;
import org.qommons.ValueHolder;
import org.qommons.collect.QuickSet.QuickMap;

public class DerivedMetricChannel<T> extends AbstractDerivedChannelType.AbstractDerivedChannel<T> {
	private final QuickMap<String, MetricDependency<T, ?>> theDependencies;
	private QuickMap<String, MetricDependency<T, ?>> theStaticDependencies;
	private QuickMap<String, MetricDependency<T, ?>> theDynamicDependencies;

	private final DerivedMetricValueCache<T> theChannelCache;

	DerivedMetricChannel(DerivedMetricChannelType<T> type, Anchor anchor, Metric<T> metric,
		QuickMap<String, MetricDependency<T, ?>> dependencies) {
		super(type, anchor, metric);

		theDependencies = dependencies.unmodifiable();
		ensureHasRequired(dependencies);
		if (!type.getDynamicDependencies().isEmpty()) {
			buildDynamicDependencies();
		} else {
			theStaticDependencies = theDependencies;
			theDynamicDependencies = type.getDynamicDependencies().createMap();
		}

		if (type.useChannelCache()) {
			theChannelCache = new DerivedMetricValueCache<>(getType(), getType().getCacheIgnoredDependencies());
		} else {
			theChannelCache = null;
		}
	}

	private void buildDynamicDependencies() {
		theStaticDependencies = getType().getStaticDependencies().createMap(theDependencies::get);
		theDynamicDependencies = getType().getDynamicDependencies().createMap(dynamicIndex -> {
			return theDependencies.get(getType().getDynamicDependencies().get(dynamicIndex));
		});
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
	public DerivedMetricChannelType<T> getType() {
		return (DerivedMetricChannelType<T>) super.getType();
	}

	public QuickMap<String, MetricDependency<T, ?>> getDependencies() {
		return theDependencies;
	}

	public T getValue(DependencyValueSet<Anchor, T> dependencyValues) {
		if (theChannelCache != null) {
			return theChannelCache.get(dependencyValues);
		}
		return getType().getValue(dependencyValues, true);
	}

	public boolean isDynamic() {
		return !getType().getDynamicDependencies().isEmpty();
	}

	@Override
	public double getCost(MetricChannelService depends) {
		double c = getType().getDerivationCost();
		if (isDynamic()) {
			ChannelBackedMDS<T> ds = new ChannelBackedMDS<>(this, depends);
			for (MetricDependency<T, ?> dep : theDependencies.values()) {
				MetricChannel<?> depChannel = ds.getChannel(dep.getName());
				if (depChannel != null) {
					c += depends.getCost(depChannel);
				}
			}
		} else {
			for (MetricDependency<T, ?> dep : theDependencies.values()) {
				MetricChannel<?> depChannel;
				depChannel = dep.getChannel();
				c += depends.getCost(depChannel);
			}
		}
		return c;
	}

	@Override
	public boolean isConstant(MetricChannelService depends) {
		if (isDynamic())
			return false;
		for (MetricDependency<T, ?> dep : theStaticDependencies.values()) {
			if (!depends.isConstant(dep.getChannel()))
				return false;
		}
		return true;
	}

	@Override
	public MetricQueryResult<T> query(MetricChannelService depends, Consumer<Object> initialListener) {
		if (isDynamic()) {
			return DynamicDerivedMetricTimeline.build(this, depends, initialListener);
		} else {
			return PassiveDerivedMetricResult.build(this, depends, initialListener);
		}
	}

	@Override
	protected int genHashCode() {
		return super.genHashCode() * 13 + theDependencies.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!super.equals(obj)) {
			return false;
		}
		return theDependencies.equals(((DerivedMetricChannel<?>) obj).theDependencies);
	}

	public static <T> DerivedMetricChannel<T> create(DerivedMetricChannelType<T> type, Anchor anchor, Metric<T> metric,
		MetricChannelService depends, Consumer<String> onError) {
		QuickMap<String, MetricDependency<T, ?>> dependencyChannels = compileDependencyChannels(type.getDependencies(), anchor, metric,
			depends, onError);
		if (dependencyChannels == null)
			return null;
		else
			return new DerivedMetricChannel<T>(type, anchor, metric, dependencyChannels);
	}

	public static <T> QuickMap<String, MetricDependency<T, ?>> compileDependencyChannels(
		QuickMap<String, MetricDependencyType<T, ?>> dependencies, Anchor mainAnchor, Metric<T> mainMetric,
		MetricChannelService dependencyService, Consumer<String> onError) {
		QuickMap<String, MetricDependency<T, ?>> dcs = dependencies.keySet().createMap();
		DependencyInstanceSetImpl<T> depSet = new DependencyInstanceSetImpl<>(mainAnchor, mainMetric);
		boolean[] failure = new boolean[1];
		for (int i = 0; i < dependencies.keySet().size(); i++) {
			MetricDependencyType<T, ?> dep = dependencies.get(i);
			MetricDependency<T, ?> instance = buildDependency(dep, mainAnchor, mainMetric, depSet, dependencyService, failure, onError);
			if (failure[0]) {
				return null;
			} else if (instance != null) {
				depSet.dcNames.add(dep.getName());
				dcs.put(i, instance);
			}
		}
		return dcs.unmodifiable();
	}

	static class DependencyInstanceSetImpl<T> implements DependencyInstanceSet<Anchor, T> {
		final Set<String> dcNames;
		private final Anchor mainAnchor;
		private final Metric<T> mainMetric;

		DependencyInstanceSetImpl(Anchor mainAnchor, Metric<T> mainMetric) {
			dcNames = new LinkedHashSet<>();
			this.mainAnchor = mainAnchor;
			this.mainMetric = mainMetric;
		}

		@Override
		public Anchor getAnchor() {
			return mainAnchor;
		}

		@Override
		public Metric<T> getMetric() {
			return mainMetric;
		}

		@Override
		public boolean isSupported(String dependencyName) {
			return dcNames.contains(dependencyName);
		}
	}

	static <T, X> MetricDependency<T, X> buildDependency(MetricDependencyType<T, X> dep, Anchor mainAnchor, Metric<T> mainMetric,
		DependencyInstanceSetImpl<T> depSet, MetricChannelService dependencyService, boolean[] failure, Consumer<String> onError) {
		DependencyType depType = dep.isRequiredForInstance(depSet);
		if (depType == DependencyType.None) {
			return null;
		}
		boolean required = depType == DependencyType.Required;
		Anchor depAnchor;
		Metric<X> depMetric;
		MetricChannel<X> channel;
		if (dep.isDynamic()) {
			depAnchor = null;
			depMetric = null;
			channel = null;
		} else {
			depAnchor = dep.getAnchor(mainAnchor, mainMetric);
			depMetric = dep.getMetric(mainAnchor, mainMetric);
			ValueHolder<String> reason = new ValueHolder<>();
			if (dep.hasChannelFilter()) {
				channel = dep.filterChannels(dependencyService.getChannels(depAnchor, depMetric, dep.isAdaptive(), reason), depSet,
					dependencyService);
			} else {
				channel = dependencyService.getChannel(depAnchor, depMetric, dep.isAdaptive(), reason);
			}
			if (channel == null) {
				failure[0] = required;
				if (required && onError != null) {
					onError.accept("Dependency " + dep.getName() + " unsupported: " + reason.get());
				}
				return null;
			}
		}
		return new MetricDependency<>(dep, depAnchor, depMetric, required, channel);
	}

	private static class PassiveDerivedMetricResult<T> extends AbstractDerivedMetricResult<T> {
		private final MetricQueryService theService;

		PassiveDerivedMetricResult(DerivedMetricChannel<T> channel, Consumer<Object> initialListener, MetricQueryService service) {
			super(channel, channel.getDependencies());
			theService = service;
			queryDependencies(initialListener, theService);
		}

		@Override
		public DerivedMetricChannel<T> getChannel() {
			return (DerivedMetricChannel<T>) super.getChannel();
		}

		@Override
		public MetricResult<T> getResult(MetricResult<T> result, MetricQueryOptions options) {
			AbstractDerivedMetricResult.ParamMapMDS<T> mds = createDS(result, options);
			if (checkAvailable(mds, result, options)) {
				result.setValue(getChannel().getValue(// Break here so we can not descend into getChannel() during debugging
					mds));
			}
			result.cacheable(mds.isCacheable());
			// BUG I have no idea why, but if this release is called, it causes concurrency issues with the maps
			// mds.release();
			return result;
		}

		static <T> PassiveDerivedMetricResult<T> build(DerivedMetricChannel<T> channel, MetricQueryService depends,
			Consumer<Object> initialListener) {
			PassiveDerivedMetricResult<T> timeline = new PassiveDerivedMetricResult<>(channel, initialListener, depends);
			return timeline;
		}
	}

	private static class StaticDS<T> extends AbstractCollection<ChannelTemplate<Object>> implements DependencyValueSet<Anchor, T> {
		private final QuickMap<String, MetricDependency<T, ?>> theDynamicDependencies;
		private final DependencyValueSet<Anchor, T> theDS;
		private final List<List<ChannelTemplate<Object>>> theChannelsByDependency;
		private final int[] theDependencyChannelIndexes;
		private int theTotalSize;

		StaticDS(QuickMap<String, MetricDependency<T, ?>> dynamicDepends, DependencyValueSet<Anchor, T> ds) {
			theDynamicDependencies = dynamicDepends;
			theDS = ds;
			int dependencies = dynamicDepends.keySet().size();
			theChannelsByDependency = new ArrayList<>(dependencies);
			theDependencyChannelIndexes = new int[dependencies];

			for (MetricDependency<T, ?> dep : dynamicDepends.allValues()) {
				if (dep == null) {
					add(Collections.emptyList());
					continue;
				}
				DependencyType type = dep.isDynamicallyRequired(ds);
				if (type == DependencyType.None) {
					add(Collections.emptyList()); // No filtering. Leave a space.
				} else {
					List<ChannelTemplate<Object>> depChannels = new ArrayList<>();
					for (Anchor anchor : dep.getType().getAnchor(ds)) {
						for (Metric<?> metric : dep.getType().getMetric(ds)) {
							ChannelTemplate<Object> template = new ChannelTemplate<>(anchor, metric, type == DependencyType.Required, //
								dep.getType().hasChannelFilter() ? (ChannelFilter<Anchor, Object>) dep.getType().filterForValues(ds)
									: null);
							depChannels.add(template);
						}
					}
					add(depChannels);
				}
			}
		}

		private void add(List<ChannelTemplate<Object>> channels) {
			theDependencyChannelIndexes[theChannelsByDependency.size()] = theTotalSize;
			theChannelsByDependency.add(channels);
			theTotalSize += channels.size();
		}

		int getChannelIndex(int dependency) {
			return theDependencyChannelIndexes[dependency];
		}

		int getChannelCount(int dependency) {
			int next = dependency < theChannelsByDependency.size() - 1 ? theDependencyChannelIndexes[dependency + 1] : theTotalSize;
			return next - theDependencyChannelIndexes[dependency];
		}

		@Override
		public Iterator<ChannelTemplate<Object>> iterator() {
			return IterableUtils.flatten(theChannelsByDependency).iterator();
		}

		@Override
		public int size() {
			return theTotalSize;
		}

		@Override
		public Anchor getAnchor() {
			return theDS.getAnchor();
		}

		@Override
		public Metric<T> getMetric() {
			return theDS.getMetric();
		}

		@Override
		public boolean isSupported(String dependencyName) {
			return theDS.isSupported(dependencyName);
		}

		@Override
		public <X> MetricChannel<X> getChannel(String dependency) {
			return theDS.getChannel(dependency);
		}

		@Override
		public <X> X get(String dependency) {
			return theDS.get(dependency);
		}
	}

	private static class DynamicDerivedMetricTimeline<T> extends DynamicDependencyResult<StaticDS<T>, Object, T> {
		DynamicDerivedMetricTimeline(Function<StaticDS<T>, ? extends Collection<? extends ChannelTemplate<Object>>> dependencyMap,
			Function<DynamicDependencySource<StaticDS<T>, Object>, ? extends T> aggregation, MetricChannel<T> channel,
			MetricQueryResult<StaticDS<T>> relationResults, MetricChannelService depends, Consumer<Object> initialListener) {
			super(dependencyMap, aggregation, channel, relationResults, depends, initialListener);
		}

		static <T> DynamicDerivedMetricTimeline<T> build(DerivedMetricChannel<T> channel, MetricChannelService depends,
			Consumer<Object> initialListener) {
			// A little type hackery to make the plumbing easier

			QuickMap<String, MetricDependency<StaticDS<T>, ?>> staticDepends;
			staticDepends = (QuickMap<String, MetricDependency<StaticDS<T>, ?>>) (QuickMap<String, ?>) channel.theStaticDependencies;
			QuickMap<String, MetricDependency<T, ?>> dynamicDepends = channel.theDynamicDependencies;
			Metric<StaticDS<T>> staticMetric = (Metric<StaticDS<T>>) channel.getMetric();
			// Taking advantage of the fact that we know the asynchronous timeline doesn't re-use ValueDependencySet instances between calls
			DerivedMetricChannel<StaticDS<T>> staticChannel;
			staticChannel = new DerivedMetricChannel<StaticDS<T>>((DerivedMetricChannelType<StaticDS<T>>) channel.getType(),
				channel.getAnchor(), staticMetric, staticDepends) {
				@Override
				public StaticDS<T> getValue(DependencyValueSet<Anchor, StaticDS<T>> dependencyValues) {
					return new StaticDS<>(dynamicDepends, (DependencyValueSet<Anchor, T>) dependencyValues);
				}
			};

			DynamicDerivedMetricTimeline<T>[] timeline = new DynamicDerivedMetricTimeline[1];
			Consumer<Object> staticUpdate = initialListener == null ? null : cause -> {
				if (timeline[0] != null) {
					timeline[0].updateRelation(cause);
				}
			};

			PassiveDerivedMetricResult<StaticDS<T>> staticTimeline = PassiveDerivedMetricResult.build(staticChannel, depends, staticUpdate);
			class DynamicDVS implements DependencyValueSet<Anchor, T> {
				private final DynamicDependencySource<StaticDS<T>, Object> theDS;

				DynamicDVS(DynamicDependencySource<StaticDS<T>, Object> ds) {
					theDS = ds;
				}

				private int getDynamicIndex(String dependency) {
					return dynamicDepends.keySet().indexOf(dependency);
				}

				@Override
				public Anchor getAnchor() {
					return channel.getAnchor();
				}

				@Override
				public Metric<T> getMetric() {
					return channel.getMetric();
				}

				@Override
				public <X> MetricChannel<X> getChannel(String dependency) {
					int dynamicIndex = getDynamicIndex(dependency);
					StaticDS<T> sds = theDS.getPrimary();
					if (dynamicIndex >= 0) {
						if (sds.getChannelCount(dynamicIndex) != 1) {
							return null;
						}
						int dynamicChannelIndex = sds.getChannelIndex(dynamicIndex);
						return (MetricChannel<X>) theDS.getChannel(dynamicChannelIndex);
					} else {
						return sds.getChannel(dependency);
					}
				}

				@Override
				public boolean isSupported(String dependency) {
					int dynamicIndex = getDynamicIndex(dependency);
					StaticDS<T> sds = theDS.getPrimary();
					if (dynamicIndex >= 0) {
						if (dynamicDepends.get(dynamicIndex).getType().isMulti()) {
							return true;
						}
						return sds.getChannelCount(dynamicIndex) > 0;
					} else {
						return sds.isSupported(dependency);
					}
				}

				@Override
				public <X> X get(String dependency) {
					int dynamicIndex = getDynamicIndex(dependency);
					StaticDS<T> sds = theDS.getPrimary();
					if (dynamicIndex >= 0) {
						int dynamicChannelIndex = sds.getChannelIndex(dynamicIndex);
						if (dynamicDepends.get(dynamicIndex).getType().isMulti()) {
							int channelCount = sds.getChannelCount(dynamicIndex);
							List<Object> values = new ArrayList<>(channelCount);
							for (int i = 0; i < channelCount; i++) {
								values.add(theDS.get(dynamicChannelIndex + 1));
							}
							return (X) values;
						}
						return (X) theDS.get(dynamicChannelIndex);
					} else {
						return sds.get(dependency);
					}
				}

				boolean isSatisfied() {
					for (MetricDependency<T, ?> dep : dynamicDepends.values()) {
						int dynamicIndex = getDynamicIndex(dep.getName());
						if (dynamicDepends.get(dynamicIndex).getType().isMulti()) {
							return true;
						}
						int channelIndex = theDS.getPrimary().getChannelIndex(dynamicIndex);
						ChannelTemplate<Object> template = theDS.getChannelTemplate(channelIndex);
						if (template != null && template.required) {
							if (theDS.get(dynamicIndex) == null) {
								return false;
							}
						}
					}
					return true;
				}
			}
			Function<DynamicDependencySource<StaticDS<T>, Object>, T> aggregation = ds -> {
				DynamicDVS dvs = new DynamicDVS(ds);
				if (dvs.isSatisfied()) {
					return channel.getValue(dvs);
				} else {
					return null;
				}
			};
			timeline[0] = new DynamicDerivedMetricTimeline<T>(//
				ds -> ds, aggregation, channel, staticTimeline, depends, initialListener);
			return timeline[0];
		}

		private static <T> ChannelCollection getDynamicChannels(DependencyValueSet<Anchor, T> ds,
			QuickMap<String, MetricDependency<T, ?>> dynamicDepends) {
			ChannelCollection channels = new ChannelCollection(dynamicDepends.keySet().size());
			for (MetricDependency<T, ?> dep : dynamicDepends.allValues()) {
				if (dep == null) {
					channels.add(Collections.emptyList());
					continue;
				}
				DependencyType type = dep.isDynamicallyRequired(ds);
				if (type == DependencyType.None) {
					channels.add(Collections.emptyList()); // No filtering. Leave a space.
				} else {
					List<ChannelTemplate<Object>> depChannels = new ArrayList<>();
					for (Anchor anchor : dep.getType().getAnchor(ds)) {
						for (Metric<?> metric : dep.getType().getMetric(ds)) {
							ChannelTemplate<Object> template = new ChannelTemplate<>(anchor, metric, type == DependencyType.Required, //
								dep.getType().hasChannelFilter() ? (ChannelFilter<Anchor, Object>) dep.getType().filterForValues(ds)
									: null);
							depChannels.add(template);
						}
					}
					channels.add(depChannels);
				}
			}
			return channels;
		}

		private static class ChannelCollection extends AbstractCollection<ChannelTemplate<Object>> {
			private final List<List<ChannelTemplate<Object>>> theChannelsByDependency;
			private final int[] theDependencyChannelIndexes;
			private int theTotalSize;

			ChannelCollection(int dependencies) {
				theChannelsByDependency = new ArrayList<>(dependencies);
				theDependencyChannelIndexes = new int[dependencies];
			}

			void add(List<ChannelTemplate<Object>> channels) {
				theDependencyChannelIndexes[theChannelsByDependency.size()] = theTotalSize;
				theChannelsByDependency.add(channels);
				theTotalSize += channels.size();
			}

			@Override
			public Iterator<ChannelTemplate<Object>> iterator() {
				return IterableUtils.flatten(theChannelsByDependency).iterator();
			}

			@Override
			public int size() {
				return theTotalSize;
			}
		}
	}

	private static class ChannelBackedMDS<T> implements DependencyValueSet<Anchor, T> {
		private final DerivedMetricChannel<T> theChannel;
		private final MetricChannelService theDependencies;
		private final Map<String, MetricChannel<?>> theDepChannels;
		private Map<String, Object> theCache;
		private Map<String, Object> theDiffCache;

		ChannelBackedMDS(DerivedMetricChannel<T> channel, MetricChannelService dependencies) {
			theChannel = channel;
			theDependencies = dependencies;
			theDepChannels = new HashMap<>();
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
			MetricDependency<T, X> dep = (MetricDependency<T, X>) theChannel.getDependencies().get(dependency);
			if (dep == null) {
				return null;
			} else if (!dep.isDynamic()) {
				return dep.getChannel();
			} else {
				return (MetricChannel<X>) theDepChannels.computeIfAbsent(dependency, n -> {
					List<? extends Anchor> anchors = dep.getType().getAnchor(this);
					if (anchors.size() != 1) {
						return null;
					}
					List<? extends Metric<X>> metrics = dep.getType().getMetric(this);
					if (metrics.size() != 1) {
						return null;
					}
					MetricChannel<X> channel;
					if (dep.getType().hasChannelFilter()) {
						channel = dep.getType().filterForValues(this)
							.selectFrom(theDependencies.getChannels(anchors.get(0), metrics.get(0), null), theDependencies);
					} else {
						channel = theDependencies.getChannel(anchors.get(0), metrics.get(0), null);
					}
					return channel;
				});
			}
		}

		@Override
		public <X> X get(String dependency) {
			if (theCache == null) {
				theCache = new HashMap<>();
			}
			return (X) theCache.computeIfAbsent(dependency, d -> {
				MetricChannel<X> channel = getChannel(dependency);
				if (channel == null) {
					return null;
				}
				MetricQueryResult<X> result = theDependencies.query(channel, null);
				return result.getValue(MetricQueryOptions.get());
			});
		}

		@Override
		public boolean isSupported(String channel) {
			return getChannel(channel) != null;
		}

		@Override
		public String toString() {
			return theChannel.toString();
		}
	}

	static Instant min(Instant t1, Instant t2) {
		return t1.compareTo(t2) <= 0 ? t1 : t2;
	}

	static Instant max(Instant t1, Instant t2) {
		return t1.compareTo(t2) >= 0 ? t1 : t2;
	}
}
