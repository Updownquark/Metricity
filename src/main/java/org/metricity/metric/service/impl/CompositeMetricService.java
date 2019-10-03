package org.metricity.metric.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

import org.dbug.DBug;
import org.dbug.DBugAnchor;
import org.dbug.DBugAnchorType;
import org.metricity.anchor.Anchor;
import org.metricity.anchor.AnchorSource;
import org.metricity.anchor.AnchorType;
import org.metricity.metric.AggregateMetric;
import org.metricity.metric.AggregateMetricType;
import org.metricity.metric.Metric;
import org.metricity.metric.MetricAggregation;
import org.metricity.metric.MetricSet;
import org.metricity.metric.MetricSupport;
import org.metricity.metric.MetricType;
import org.metricity.metric.MetricTypeBuilder;
import org.metricity.metric.MultiRelationMetricType;
import org.metricity.metric.RelatedMetric;
import org.metricity.metric.RelatedMetricType;
import org.metricity.metric.RelationMetricType;
import org.metricity.metric.SimpleMetric;
import org.metricity.metric.SimpleMetricType;
import org.metricity.metric.StandardAggregateFunctions;
import org.metricity.metric.service.MetricChannel;
import org.metricity.metric.service.MetricChannelService;
import org.metricity.metric.service.MetricChannelServiceComponent;
import org.metricity.metric.service.MetricQueryResult;
import org.metricity.metric.service.MetricQueryService;
import org.metricity.metric.service.MetricTag;
import org.metricity.metric.util.SmartMetricChannel;
import org.metricity.metric.util.StringAppendingErrorHandler;
import org.observe.ObservableValue;
import org.observe.SimpleSettableValue;
import org.observe.Subscription;
import org.observe.util.TypeTokens;
import org.qommons.BiTuple;
import org.qommons.ComparableBiTuple;
import org.qommons.IterableUtils;
import org.qommons.Transaction;
import org.qommons.collect.BetterHashMap;
import org.qommons.collect.BetterHashSet;
import org.qommons.collect.BetterList;
import org.qommons.collect.CollectionElement;
import org.qommons.collect.ElementId;
import org.qommons.collect.MapEntryHandle;
import org.qommons.collect.MutableCollectionElement;
import org.qommons.tree.SortedTreeList;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.reflect.TypeToken;

/**
 * The main {@link MetricChannelService} implementation. Accepts {@link MetricChannelServiceComponent components} and glues all the
 * dependencies together
 */
public class CompositeMetricService implements MetricChannelService {
	private static final int RELATIVE_METRIC_DEPTH = 3;
	private static final DBugAnchorType<CompositeMetricService> DEBUG_TYPE = DBug.declare("metricity", CompositeMetricService.class, b -> b//
		.withEvent("getChannelFail", eb -> eb//
			.withEventField("anchor", TypeTokens.get().of(Anchor.class))//
			.withEventField("metric", new TypeToken<Metric<?>>() {})//
			.withEventField("anyExcluded", TypeTokens.get().BOOLEAN)//
			.withEventField("componentClass", TypeTokens.get().STRING)//
			.withEventField("reason", TypeTokens.get().STRING))//
	);
	private final DBugAnchor<CompositeMetricService> debug = DEBUG_TYPE.debug(this).build();

	private static class ComponentHolder {
		final MetricChannelServiceComponent component;
		Subscription supportChangesSub;

		ComponentHolder(MetricChannelServiceComponent component) {
			this.component = component;
		}
	}

	private final MetricChannelService theParent;
	private final SortedTreeList<ComponentHolder> theComponents;
	private final ReentrantReadWriteLock theComponentLock;
	private final Map<MetricChannelServiceComponent, ElementId> theComponentDependencies;
	private final AtomicInteger theSupportChanging;
	private final SimpleSettableValue<CompositeMetricSupport> theSupport;
	private final ObservableValue<MetricSupport> theExposedSupport;

	private final ThreadLocal<ComponentDependencyService> theDependencyServices = ThreadLocal
		.withInitial(() -> new ComponentDependencyService(this));

	private final MetricTimelineCache theCache;
	private final Cache<BiTuple<Anchor, Metric<?>>, ComponentChannel<?>> theChannelCache;

	/**
	 * Creates the metric service
	 * 
	 * @param parent The parent for this service
	 * @param cache Whether this service should use a result cache
	 */
	public CompositeMetricService(MetricChannelService parent, boolean cache) {
		theParent = parent;
		theComponents = new SortedTreeList<>(false, (c1, c2) -> -(c2.component.getPriority() - c1.component.getPriority()));
		theComponentLock = new ReentrantReadWriteLock();
		theComponentDependencies = new IdentityHashMap<>();
		theCache = cache ? new MetricTimelineCache() : null;

		theSupportChanging = new AtomicInteger();
		theSupport = new SimpleSettableValue<>(CompositeMetricSupport.class, false);
		theExposedSupport = theSupport.map(TypeTokens.get().of(MetricSupport.class), ms -> ms, opts -> opts.cache(false));
		theChannelCache = CacheBuilder.newBuilder()//
			.concurrencyLevel(8)//
			.maximumSize(10000)//
			.softValues()//
			.build();
	}

	/**
	 * @param component the component to register
	 * @return This service
	 */
	public CompositeMetricService registerComponent(MetricChannelServiceComponent component) {
		theSupportChanging.incrementAndGet();
		Lock lock = theComponentLock.writeLock();
		lock.lock();
		try {
			ComponentHolder holder = new ComponentHolder(component);
			ElementId el = theComponents.addElement(holder, false).getElementId();
			theComponentDependencies.put(component, el);
			component.initialize(this);
			holder.supportChangesSub = component.supportChanges().act(v -> {
				if (theSupportChanging.getAndIncrement() == 0) {
					theSupport.set(new CompositeMetricSupport(theComponents), v);
				}
				theSupportChanging.decrementAndGet();
			});
		} finally {
			lock.unlock();
			if (theSupportChanging.decrementAndGet() == 0) {
				theCache.clearValueCache();
				theSupport.set(new CompositeMetricSupport(theComponents), null);
			}
		}
		return this;
	}

	/**
	 * @param component the component to deregister
	 */
	public void removeComponent(MetricChannelServiceComponent component) {
		theSupportChanging.incrementAndGet();
		Lock lock = theComponentLock.writeLock();
		lock.lock();
		try {
			ElementId elId = theComponentDependencies.remove(component);
			if (elId != null) {
				MutableCollectionElement<ComponentHolder> el = theComponents.mutableElement(elId);
				el.get().supportChangesSub.unsubscribe();
				el.remove();
			}
		} finally {
			lock.unlock();
			if (theSupportChanging.decrementAndGet() == 0) {
				theCache.clearValueCache();
				theSupport.set(new CompositeMetricSupport(theComponents), null);
			}
		}
	}

	<T> T onDS(ElementId componentId, Anchor excludeAnchor, Metric<?> excludeMetric, Function<MetricChannelService, T> action) {
		ComponentDependencyService ds = theDependencyServices.get();
		try (Transaction t = ds.exclude(componentId, excludeAnchor, excludeMetric)) {
			return action.apply(ds);
		}
	}

	<T> T onDS(ElementId componentId, MetricChannel<?> channel, Function<MetricChannelService, T> action) {
		return onDS(componentId, channel == null ? null : channel.getAnchor(), channel == null ? null : channel.getMetric(), action);
	}

	@Override
	public ObservableValue<MetricSupport> getSupport() {
		return theExposedSupport;
	}

	@Override
	public boolean isMetricSupported(AnchorSource anchorSource, AnchorType<?> anchorType, MetricType<?> metric) {
		if (metric instanceof RelatedMetricType) {
			RelatedMetricType<?> rel = (RelatedMetricType<?>) metric;
			return isMetricSupported(anchorSource, anchorType, rel.getRelationMetricType())//
				&& isMetricSupported(anchorSource, rel.getRelationMetricType().getTargetAnchorType(), rel.getRelativeMetricType());
		} else if (metric instanceof AggregateMetricType) {
			AggregateMetricType<?, ?> agg = (AggregateMetricType<?, ?>) metric;
			MetricType<?> relative = agg.getRelativeMetricType();
			return isMetricSupported(anchorSource, anchorType, agg.getRelationMetricType())//
				&& isMetricSupported(anchorSource, agg.getRelationMetricType().getTargetAnchorType(), relative);
		} else if (metric instanceof SimpleMetricType) {
			return theSupport.get().get(anchorSource, anchorType).isSupported((SimpleMetricType<?>) metric);
		} else {
			return false;
		}
	}

	@Override
	public Set<MetricType<?>> getAllSupportedMetrics(AnchorSource anchorSource, AnchorType<?> anchorType) {
		Set<MetricType<?>> metrics = new LinkedHashSet<>();
		MetricSupport support = theSupport.get();
		metrics.addAll(support.getAllNonAggregate(anchorSource, anchorType));
		for (BiTuple<MultiRelationMetricType<?, ?>, ? extends MetricType<?>> aggType : support.getAllAggregate(anchorSource, anchorType)) {
			for (MetricAggregation<?, ?> agg : StandardAggregateFunctions.getStandardAggregations(aggType.getValue2())) {
				metrics.add(MetricTypeBuilder.buildAggregate(aggType.getValue1(), (MetricType<Object>) aggType.getValue2(),
					(MetricAggregation<Object, ?>) agg));
			}
		}
		return Collections.unmodifiableSet(metrics);
	}

	@Override
	public <T> MetricChannel<T> getChannel(Anchor anchor, Metric<T> metric, boolean allowRecursion, Consumer<String> onError) {
		return getChannel(anchor, metric, null, onError);
	}

	@Override
	public <T> List<? extends MetricChannel<T>> getChannels(Anchor anchor, Metric<T> metric, boolean allowRecursion,
		Consumer<String> onError) {
		return getChannels(anchor, metric, null, onError);
	}

	private <T> MetricChannel<T> getChannel(Anchor anchor, Metric<T> metric, Set<ElementId> otherThan, Consumer<String> onError) {
		if (anchor == null || metric == null) {
			throw new NullPointerException();
		}
		if (metric instanceof RelatedMetric) {
			RelatedMetric<T> rMetric = (RelatedMetric<T>) metric;
			MetricChannel<? extends Anchor> anchorChannel = getChannel(anchor, rMetric.getRelationMetric(), onError);
			return anchorChannel == null ? null : RelatedMetricChannel.createRelatedChannel(anchor, anchorChannel, rMetric);
		} else if (metric instanceof AggregateMetric) {
			AggregateMetric<Anchor, T> rMetric = (AggregateMetric<Anchor, T>) metric;
			MetricChannel<? extends Collection<? extends Anchor>> anchorChannel = getChannel(anchor, rMetric.getRelationMetric(), onError);
			return anchorChannel == null ? null : AggregateMetricChannel.createAggregateChannel(anchor, anchorChannel, rMetric);
		} else if (!(metric instanceof SimpleMetric)) {
			throw new IllegalArgumentException("Unrecognized metric class: " + metric.getClass().getName());
		}
		Lock lock = theComponentLock.readLock();
		lock.lock();
		try {
			CompositeMetricSupport support = theSupport.get();
			MetricChannel<T> channel = _getChannel(support, anchor, metric, otherThan, onError);
			if (channel != null)
				return channel;
		} finally {
			lock.unlock();
		}
		if (theParent != null) {
			MetricChannel<T> channel = theParent.getChannel(anchor, metric, onError);
			if (channel != null)
				return new ParentSourcedChannel<>(channel);
		}
		return null;
	}

	private <T> MetricChannel<T> _getChannel(CompositeMetricSupport support, Anchor anchor, Metric<T> metric, Set<ElementId> otherThan,
		Consumer<String> onError) {
		BiTuple<Anchor, Metric<?>> key;
		if (otherThan == null) {
			ComponentChannel<T> cachedChannel;
			key = new BiTuple<>(anchor, metric);
			cachedChannel = (ComponentChannel<T>) theChannelCache.getIfPresent(key);
			if (cachedChannel != null) {
				return cachedChannel;
			}
		} else {
			key = null;
		}
		// Compile support for the anchor
		Set<ElementId> components = support.getOrCompile(anchor.getSource(), anchor.getType()).theMetrics.get(metric.getType());
		if (components == null) {
			debug.event("getChannelFail").with("anchor", anchor).with("metric", metric).with("componentClass", null)
				.with("anyExcluded", otherThan != null).with("reason", "Unsupported by any component").occurred();
			return null; // Unsupported by any component
		}
		StringAppendingErrorHandler reason = (debug.isActive() || onError != null) ? new StringAppendingErrorHandler(", ") : null;
		for (ElementId componentId : components) {
			MetricChannelServiceComponent component = theComponents.getElement(componentId).get().component;
			Consumer<String> cReason = reason == null ? null : reason.withPrepend(component.getClass().getSimpleName(), ": ");
			if (otherThan != null && otherThan.contains(componentId)) {
				if (cReason != null) {
					cReason.accept("recursively excluded");
				}
				continue;
			}
			MetricChannel<T> channel = onDS(componentId, anchor, metric, ds -> {
				return component.getChannel(anchor, metric, ds, cReason);
			});
			if (channel != null) {
				ComponentChannel<T> cachedChannel = wrap(channel, componentId, component);
				if (otherThan == null) {
					try {
						return (MetricChannel<T>) theChannelCache.get(key, () -> cachedChannel);
					} catch (ExecutionException e) {
						System.err.println("Should not happen");
						e.printStackTrace();
					}
				} else {
					return cachedChannel;
				}
			} else {
				debug.event("getChannelFail").with("anchor", anchor).with("metric", metric)
					.with("componentClass", component.getClass().getName()).with("anyExcluded", otherThan != null)
					.with("reason", reason::toString).occurred();
			}
		}
		if (onError != null) {
			onError.accept(reason.borderWith("{", "}").toString());
		}
		return null;
	}

	private <T> List<? extends MetricChannel<T>> getChannels(Anchor anchor, Metric<T> metric, Set<ElementId> otherThan,
		Consumer<String> onError) {
		if (anchor == null || metric == null) {
			throw new NullPointerException();
		}
		if (metric instanceof RelatedMetric) {
			RelatedMetric<T> rMetric = (RelatedMetric<T>) metric;
			MetricChannel<? extends Anchor> anchorChannel = getChannel(anchor, rMetric.getRelationMetric(), onError);
			return anchorChannel == null ? Collections.emptyList()
				: Arrays.asList(RelatedMetricChannel.createRelatedChannel(anchor, anchorChannel, rMetric));
		} else if (metric instanceof AggregateMetric) {
			AggregateMetric<Anchor, T> rMetric = (AggregateMetric<Anchor, T>) metric;
			MetricChannel<? extends Collection<? extends Anchor>> anchorChannel = getChannel(anchor, rMetric.getRelationMetric(), onError);
			return anchorChannel == null ? Collections.emptyList()
				: Arrays.asList(AggregateMetricChannel.createAggregateChannel(anchor, anchorChannel, rMetric));
		} else if (!(metric instanceof SimpleMetric)) {
			throw new IllegalArgumentException("Unrecognized metric class: " + metric.getClass().getName());
		}
		Lock lock = theComponentLock.readLock();
		lock.lock();
		try {
			CompositeMetricSupport support = theSupport.get();
			// Compile support for the anchor
			Set<ElementId> components = support.getOrCompile(anchor.getSource(), anchor.getType()).theMetrics.get(metric.getType());
			if (components == null) {
				debug.event("getChannelFail").with("anchor", anchor).with("metric", metric).with("componentClass", null)
					.with("anyExcluded", otherThan != null).with("reason", "Unsupported by any component").occurred();
				return Collections.emptyList(); // Unsupported by any component
			}
			List<MetricChannel<T>> allChannels = new ArrayList<>(components.size());
			StringAppendingErrorHandler reason = (debug.isActive() || onError != null) ? new StringAppendingErrorHandler(", ") : null;
			for (ElementId componentId : components) {
				if (otherThan != null && otherThan.contains(componentId)) {
					continue;
				}
				MetricChannelServiceComponent component = theComponents.getElement(componentId).get().component;
				Consumer<String> cReason = reason == null ? null : reason.withPrepend(component.getClass().getSimpleName(), ": ");
				List<MetricChannel<T>> channels = onDS(componentId, anchor, metric,
					ds -> (List<MetricChannel<T>>) component.getChannels(anchor, metric, ds, cReason));
				if (!channels.isEmpty()) {
					for (MetricChannel<T> channel : channels) {
						if (channel != null) {
							allChannels.add(wrap(channel, componentId, component));
						}
					}
				} else {
					debug.event("getChannelFail").with("anchor", anchor).with("metric", metric)
						.with("componentClass", component.getClass().getName()).with("anyExcluded", otherThan != null)
						.with("reason", reason::toString).occurred();
				}
			}
			if (allChannels.isEmpty() && onError != null) {
				onError.accept(reason.toString());
			}
			return Collections.unmodifiableList(allChannels);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public double getCost(MetricChannel<?> channel) {
		if (channel == null) {
			throw new NullPointerException("Null channel");
		} else if (channel instanceof ParentSourcedChannel) {
			return theParent.getCost(((ParentSourcedChannel<?>) channel).theParentChannel);
		} else if (channel instanceof ComponentChannel) {
			ComponentChannel<?> cc = (ComponentChannel<?>) channel;
			if (cc.channel instanceof SmartMetricChannel) {
				return onDS(cc.componentEl, cc.channel,
					ds -> ((SmartMetricChannel<?>) cc.channel).getCost(ds));
			}
			return ((ComponentChannel<?>) channel).component.getCost(((ComponentChannel<?>) channel).channel);
		} else if (channel instanceof SmartMetricChannel) {
			return ((SmartMetricChannel<?>) channel).getCost(this);
		} else {
			throw new IllegalArgumentException("Unrecognized channel implementation: " + channel.getClass().getName());
		}
	}

	@Override
	public Set<MetricTag> getTags(MetricChannel<?> channel) {
		if (channel == null) {
			throw new NullPointerException("Null channel");
		} else if (channel instanceof ParentSourcedChannel) {
			return theParent.getTags(((ParentSourcedChannel<?>) channel).theParentChannel);
		} else if (channel instanceof ComponentChannel) {
			ComponentChannel<?> cc = (ComponentChannel<?>) channel;
			if (cc.channel instanceof SmartMetricChannel) {
				return onDS(cc.componentEl, cc.channel, ds -> ((SmartMetricChannel<?>) cc.channel).getTags(ds));
			}
			return ((ComponentChannel<?>) channel).component.getTags(((ComponentChannel<?>) channel).channel);
		} else if (channel instanceof SmartMetricChannel) {
			return ((SmartMetricChannel<?>) channel).getTags(this);
		} else {
			throw new IllegalArgumentException("Unrecognized channel implementation: " + channel.getClass().getName());
		}
	}

	@Override
	public boolean isConstant(MetricChannel<?> channel) {
		if (channel == null) {
			throw new NullPointerException("Null channel");
		} else if (channel instanceof ParentSourcedChannel) {
			return theParent.isConstant(((ParentSourcedChannel<?>) channel).theParentChannel);
		} else if (channel instanceof ComponentChannel) {
			ComponentChannel<?> cc = (ComponentChannel<?>) channel;
			if (cc.channel instanceof SmartMetricChannel) {
				return onDS(cc.componentEl, cc.channel, ds -> ((SmartMetricChannel<?>) cc.channel).isConstant(ds));
			}
			return ((ComponentChannel<?>) channel).component.isConstant(((ComponentChannel<?>) channel).channel);
		} else if (channel instanceof SmartMetricChannel) {
			return ((SmartMetricChannel<?>) channel).isConstant(this);
		} else {
			throw new IllegalArgumentException("Unrecognized channel implementation: " + channel.getClass().getName());
		}
	}

	@Override
	public <T> MetricQueryResult<T> query(MetricChannel<T> channel, Consumer<Object> updateReceiver) {
		if (channel instanceof ParentSourcedChannel)
			return theParent.query(((ParentSourcedChannel<T>) channel).theParentChannel, updateReceiver);
		else if (theCache != null)
			return theCache.query(channel, updateReceiver, new DoOneDirectly());
		else
			return _query(channel, updateReceiver);
	}

	<T> MetricQueryResult<T> _query(MetricChannel<T> channel, Consumer<Object> updateReceiver) {
		if (channel == null) {
			throw new NullPointerException("Null channel");
		} else if (channel instanceof ComponentChannel) {
			ComponentChannel<T> cc = (ComponentChannel<T>) channel;
			if (cc.channel instanceof SmartMetricChannel) {
				SmartMetricChannel<T> smart = (SmartMetricChannel<T>) cc.channel;
				return this.onDS(cc.componentEl, cc.channel, ds -> smart.query(this, updateReceiver));
			} else {
				return cc.component.query(cc.channel, updateReceiver);
			}
		} else if (channel instanceof SmartMetricChannel) {
			SmartMetricChannel<T> smart = (SmartMetricChannel<T>) channel;
			return smart.query(this, updateReceiver);
		} else {
			throw new IllegalArgumentException("Unrecognized channel implementation: " + channel.getClass().getName());
		}
	}

	private static <T> ComponentChannel<T> wrap(MetricChannel<T> componentChannel, ElementId componentEl,
		MetricChannelServiceComponent component) {
		if (componentChannel == null)
			return null;
		return new NormalComponentChannel<>(component, componentEl, componentChannel);
	}

	@Override
	public MetricChannelService derived(MetricChannelServiceComponent metrics, boolean withCaching) {
		CompositeMetricService newMCS = new CompositeMetricService(this, withCaching);
		newMCS.registerComponent(metrics);
		return newMCS;
	}

	private static class ComponentDependencyService implements MetricChannelService {
		private final CompositeMetricService theParent;
		private final List<InUseSet> theInactiveSets;
		// private final List<ElementId>
		private final LinkedList<RecursionDetection> theRecursionDetection;

		ComponentDependencyService(CompositeMetricService parent) {
			theParent = parent;
			theInactiveSets = new ArrayList<>(100);
			theRecursionDetection = new LinkedList<>();
			theRecursionDetection.add(new RecursionDetection(theInactiveSets));
		}

		Transaction exclude(ElementId component, Anchor anchor, Metric<?> metric) {
			return theRecursionDetection.getLast().use(component, anchor, metric);
		}

		@Override
		public <T> MetricChannel<T> getChannel(Anchor anchor, Metric<T> metric, boolean allowRecursion, Consumer<String> onError) {
			if (allowRecursion) {
				theRecursionDetection.add(new RecursionDetection(theInactiveSets));
			}

			MetricChannel<T> channel = theParent.getChannel(anchor, metric, theRecursionDetection.getLast().getExcluded(anchor, metric),
				onError);
			if (allowRecursion) {
				theRecursionDetection.removeLast();
			}
			return channel;
		}

		@Override
		public <T> List<? extends MetricChannel<T>> getChannels(Anchor anchor, Metric<T> metric, boolean allowRecursion,
			Consumer<String> onError) {
			if (allowRecursion) {
				theRecursionDetection.add(new RecursionDetection(theInactiveSets));
			}
			List<? extends MetricChannel<T>> channels = theParent.getChannels(anchor, metric,
				theRecursionDetection.getLast().getExcluded(anchor, metric), onError);
			if (allowRecursion) {
				theRecursionDetection.removeLast();
			}
			return channels;
		}

		@Override
		public double getCost(MetricChannel<?> channel) {
			return theParent.getCost(channel);
		}

		@Override
		public Set<MetricTag> getTags(MetricChannel<?> channel) {
			return theParent.getTags(channel);
		}

		@Override
		public boolean isConstant(MetricChannel<?> channel) {
			return theParent.isConstant(channel);
		}

		@Override
		public <T> MetricQueryResult<T> query(MetricChannel<T> channel, Consumer<Object> updateReceiver) {
			return theParent.query(channel, updateReceiver);
		}

		@Override
		public ObservableValue<MetricSupport> getSupport() {
			// We may at some point need to make a way to determine support with this component excluded
			return theParent.getSupport();
		}

		@Override
		public boolean isMetricSupported(AnchorSource anchorSource, AnchorType<?> anchorType, MetricType<?> metric) {
			// Shouldn't be needed, right? If, so, we need to make a way to determine support with this component excluded
			throw new IllegalStateException("Not legal to call this");
		}

		@Override
		public Set<MetricType<?>> getAllSupportedMetrics(AnchorSource anchorSource, AnchorType<?> anchorType) {
			// Shouldn't be needed, right? If, so, we need to make a way to determine support with this component excluded
			throw new IllegalStateException("Not legal to call this");
		}

		@Override
		public MetricChannelService derived(MetricChannelServiceComponent metrics, boolean withCaching) {
			// Shouldn't be needed
			throw new IllegalStateException("Not legal to call this");
		}
	}

	static class RecursionDetection {
		private final List<InUseSet> theInactiveSets;
		private final BetterHashMap<BiTuple<Anchor, Metric<?>>, InUseSet> inUse;

		boolean added;
		Set<ElementId> excluded;

		RecursionDetection(List<InUseSet> inactiveSets) {
			theInactiveSets = new ArrayList<>(20);
			inUse = BetterHashMap.build().unsafe().buildMap();
		}

		Transaction use(ElementId component, Anchor anchor, Metric<?> metric) {
			added = false;
			MapEntryHandle<?, InUseSet> entry = inUse.getOrPutEntry(new BiTuple<>(anchor, metric), am -> {
				if (!theInactiveSets.isEmpty()) {
					return theInactiveSets.remove(theInactiveSets.size() - 1);
				} else {
					return new InUseSet();
				}
			}, false, () -> added = true);
			Transaction inUseT = entry.getValue().use(component);
			excluded = entry.getValue().excluded;
			if (added) {
				return () -> {
					inUseT.close();
					theInactiveSets.add(entry.getValue());
					inUse.mutableEntry(entry.getElementId()).remove();
				};
			} else {
				return inUseT;
			}
		}

		Set<ElementId> getExcluded(Anchor anchor, Metric<?> metric) {
			InUseSet iu = inUse.get(new BiTuple<>(anchor, metric));
			return iu == null ? null : iu.excluded;
		}
	}

	static class InUseSet {
		private final BetterHashSet<ElementId> inUse;
		final BetterHashSet<ElementId> excluded;

		InUseSet() {
			inUse = BetterHashSet.build().unsafe().buildSet();
			excluded = BetterHashSet.build().unsafe().buildSet();
		}

		Transaction use(ElementId component) {
			CollectionElement<?> addedInUse = inUse.addElement(component, false);
			if (addedInUse == null) { // Already in use. Add to the excluded set.
				CollectionElement<ElementId> addedExcluded = excluded.addElement(component, false);
				if (addedExcluded == null) {
					return Transaction.NONE;
				}
				return () -> {
					excluded.mutableElement(addedExcluded.getElementId()).remove();
				};
			} else {
				return () -> {
					inUse.mutableElement(addedInUse.getElementId()).remove();
				};
			}
		}
	}

	private class DoOneDirectly implements MetricQueryService {
		private volatile boolean used;

		@Override
		public double getCost(MetricChannel<?> channel) {
			return CompositeMetricService.this.getCost(channel);
		}

		@Override
		public Set<MetricTag> getTags(MetricChannel<?> channel) {
			return CompositeMetricService.this.getTags(channel);
		}

		@Override
		public boolean isConstant(MetricChannel<?> channel) {
			return CompositeMetricService.this.isConstant(channel);
		}

		@Override
		public <T> MetricQueryResult<T> query(MetricChannel<T> channel, Consumer<Object> updateReceiver) {
			if (!used) {
				used = true;
				// Query directly, no caching
				return _query(channel, updateReceiver);
			} else {
				return CompositeMetricService.this.query(channel, updateReceiver);
			}
		}
	}

	private static abstract class ComponentChannel<T> implements MetricChannel<T> {
		final MetricChannelServiceComponent component;
		final ElementId componentEl;
		final MetricChannel<T> channel;
		final Anchor anchor;

		ComponentChannel(MetricChannelServiceComponent component, ElementId componentEl, MetricChannel<T> channel) {
			this.component = component;
			this.componentEl = componentEl;
			this.channel = channel;
			this.anchor = channel.getAnchor();
		}

		@Override
		public Anchor getAnchor() {
			return anchor;
		}

		@Override
		public abstract Metric<T> getMetric();

		@Override
		public boolean isRecursive() {
			return channel.isRecursive();
		}

		@Override
		public int hashCode() {
			return channel.hashCode();
		}

		@Override
		public boolean equals(Object o) {
			return o instanceof ComponentChannel && channel.equals(((ComponentChannel<?>) o).channel);
		}

		@Override
		public String toString() {
			return channel.toString();
		}
	}

	private static class NormalComponentChannel<T> extends ComponentChannel<T> {
		private final Metric<T> metric;

		NormalComponentChannel(MetricChannelServiceComponent component, ElementId componentEl, MetricChannel<T> channel) {
			super(component, componentEl, channel);
			metric = channel.getMetric();
		}

		@Override
		public Metric<T> getMetric() {
			return metric;
		}
	}

	private static class ParentSourcedChannel<T> implements MetricChannel<T> {
		final MetricChannel<T> theParentChannel;

		ParentSourcedChannel(MetricChannel<T> parentChannel) {
			theParentChannel = parentChannel;
		}

		@Override
		public Anchor getAnchor() {
			return theParentChannel.getAnchor();
		}

		@Override
		public Metric<T> getMetric() {
			return theParentChannel.getMetric();
		}

		@Override
		public boolean isRecursive() {
			return theParentChannel.isRecursive();
		}

		@Override
		public int hashCode() {
			return theParentChannel.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof ParentSourcedChannel)
				obj = ((ParentSourcedChannel<?>) obj).theParentChannel;
			return theParentChannel.equals(obj);
		}

		@Override
		public String toString() {
			return theParentChannel.toString();
		}
	}

	// Metric satisfaction data structures

	static class MetricSetImpl<M extends SimpleMetricType<?>> implements MetricSet<M> {
		private final Set<M> allMetrics;
		private final Set<M> theExposedMetrics;
		private final Map<String, Deque<M>> theMetricsByName;

		MetricSetImpl() {
			allMetrics = new LinkedHashSet<>();
			theExposedMetrics = new LinkedHashSet<>();
			theMetricsByName = new HashMap<>();
		}

		private void add(M metric) {
			allMetrics.add(metric);
			if (!metric.isInternalOnly()) {
				if (theExposedMetrics.add(metric)) {
					theMetricsByName.computeIfAbsent(metric.getName(), n -> new LinkedList<>()).add(metric);
				}
			}
		}

		@Override
		public Set<M> getAllSupported() {
			return Collections.unmodifiableSet(theExposedMetrics);
		}

		@Override
		public boolean isSupported(SimpleMetricType<?> metric) {
			return allMetrics.contains(metric);
		}

		@Override
		public Deque<M> get(String metricName) {
			Deque<M> found = theMetricsByName.get(metricName);
			return found == null ? emptyDeque() : unmodifiableDeque(found);
		}
	}

	static class MetricAvailabilityAnchorQueryState {
		final Map<SimpleMetricType<?>, Set<ElementId>> theMetrics;
		final MetricSetImpl<SimpleMetricType<?>> theAllMetrics;
		final MetricSetImpl<RelationMetricType.Simple<?>> theRelationMetrics;
		final MetricSetImpl<MultiRelationMetricType.Simple<?, ?>> theMultiRelationMetrics;
		final Set<MetricType<?>> theAllNonAgg;
		final Set<BiTuple<MultiRelationMetricType<?, ?>, SimpleMetricType<?>>> theAllAgg;

		MetricAvailabilityAnchorQueryState() {
			theMetrics = new LinkedHashMap<>();
			theAllMetrics = new MetricSetImpl<>();
			theRelationMetrics = new MetricSetImpl<>();
			theMultiRelationMetrics = new MetricSetImpl<>();
			theAllNonAgg = new LinkedHashSet<>();
			theAllAgg = new LinkedHashSet<>();
		}

		boolean addSupport(SimpleMetricType<?> metric, ElementId component) {
			boolean added = theMetrics.computeIfAbsent(metric, m -> new LinkedHashSet<>()).add(component);
			if (!added) {
				return false;
			}
			theAllMetrics.add(metric);
			if (metric instanceof RelationMetricType.Simple) {
				theRelationMetrics.add((RelationMetricType.Simple<?>) metric);
			}
			if (metric instanceof MultiRelationMetricType.Simple) {
				theMultiRelationMetrics.add((MultiRelationMetricType.Simple<?, ?>) metric);
			}
			return true;
		}

		MetricSet<SimpleMetricType<?>> get() {
			return theAllMetrics;
		}

		MetricSet<RelationMetricType.Simple<?>> getRelational() {
			return theRelationMetrics;
		}

		MetricSet<MultiRelationMetricType.Simple<?, ?>> getMultiRelational() {
			return theMultiRelationMetrics;
		}
	}

	static class CompositeMetricSupport implements MetricSupport {
		private final ReentrantLock theLock;
		private final Map<ElementId, MetricChannelServiceComponent> theComponents;
		private final NavigableMap<ComparableBiTuple<AnchorSource, AnchorType<?>>, MetricAvailabilityAnchorQueryState> theStates;
		private final Map<ComparableBiTuple<AnchorSource, AnchorType<?>>, Integer> theCompilingDependencies;
		private int theCompileIteration;
		private boolean wasModified;

		CompositeMetricSupport(BetterList<ComponentHolder> components) {
			theLock = new ReentrantLock();
			theComponents = new LinkedHashMap<>(components.size());
			try (Transaction t = components.lock(false, null)) {
				components.spliterator().forEachElement(el -> {
					theComponents.put(el.getElementId(), el.get().component);
				}, true);
			}
			theCompilingDependencies = new LinkedHashMap<>();
			theStates = new TreeMap<>();
		}

		MetricAvailabilityAnchorQueryState getOrCompile(AnchorSource anchorSource, AnchorType<?> anchorType) {
			theLock.lock();
			try {
				ComparableBiTuple<AnchorSource, AnchorType<?>> key = new ComparableBiTuple<>(anchorSource, anchorType);
				MetricAvailabilityAnchorQueryState state = theStates.get(key);
				// This boolean is for whether this source/type key is a dependency of the top-level source being compiled currently
				boolean isCompilingDependency = false;
				if (state == null) {
					state = new MetricAvailabilityAnchorQueryState();
					theStates.put(key, state);
					if (theCompileIteration == 0) {
						try {
							compile(anchorSource, anchorType, state);
						} finally {
							theCompileIteration = 0;
						}
					} else {
						isCompilingDependency = true;
						theCompilingDependencies.put(key, theCompileIteration);
					}
				} else {
					// If this dependency was already compiled prior to the current compilation,
					// then it's complete and doesn't need compiling
					// Also, we only want to compile each dependency once per iteration
					Integer pulledI = theCompilingDependencies.get(key);
					isCompilingDependency = pulledI != null && pulledI.intValue() != theCompileIteration;
					if (isCompilingDependency) {
						theCompilingDependencies.put(key, theCompileIteration);
					}
				}
				if (isCompilingDependency) {
					pullSupportFromComposites(anchorSource, anchorType, state);
				}
				return state;
			} finally {
				theLock.unlock();
			}
		}

		private void pullSupportFromComposites(AnchorSource anchorSource, AnchorType<?> anchorType,
			MetricAvailabilityAnchorQueryState state) {
			for (Map.Entry<ElementId, MetricChannelServiceComponent> component : theComponents.entrySet()) {
				Collection<SimpleMetricType<?>> metrics = component.getValue().getAllSupportedMetrics(anchorSource, anchorType, this);
				for (SimpleMetricType<?> metric : metrics) {
					if (state.addSupport(metric, component.getKey())) {
						wasModified = true;
					}
				}
			}
		}

		private void compile(AnchorSource anchorSource, AnchorType<?> anchorType, MetricAvailabilityAnchorQueryState state) {
			// Need to repeatedly pull support from composite sources, accumulating support with dependencies
			do {
				wasModified = false;
				theCompileIteration++;
				theCompilingDependencies.put(new ComparableBiTuple<>(anchorSource, anchorType), theCompileIteration);
				pullSupportFromComposites(anchorSource, anchorType, state);
			} while (wasModified);
			theCompilingDependencies.clear();
		}

		@Override
		public MetricSet<SimpleMetricType<?>> get(AnchorSource anchorSource, AnchorType<?> anchorType) {
			return getOrCompile(anchorSource, anchorType).get();
		}

		@Override
		public MetricSet<RelationMetricType.Simple<?>> getRelational(AnchorSource anchorSource, AnchorType<?> anchorType) {
			return getOrCompile(anchorSource, anchorType).getRelational();
		}

		@Override
		public MetricSet<MultiRelationMetricType.Simple<?, ?>> getMultiRelational(AnchorSource anchorSource, AnchorType<?> anchorType) {
			return getOrCompile(anchorSource, anchorType).getMultiRelational();
		}

		@Override
		public Set<MetricType<?>> getAllNonAggregate(AnchorSource anchorSource, AnchorType<?> anchorType) {
			theLock.lock();
			try {
				MetricAvailabilityAnchorQueryState state = getOrCompile(anchorSource, anchorType);
				if (state.theAllNonAgg.isEmpty()) {
					List<RelationMetricType<?>> relations = new ArrayList<>();
					List<RelationMetricType<?>> relationCopy = new ArrayList<>();
					state.theAllNonAgg.addAll(state.theAllMetrics.theExposedMetrics);
					relations.addAll(state.theRelationMetrics.theExposedMetrics);
					for (int depth = 1; depth < RELATIVE_METRIC_DEPTH; depth++) {
						relationCopy.clear();
						relationCopy.addAll(relations);
						relations.clear();
						for (RelationMetricType<?> relation : relationCopy) {
							MetricAvailabilityAnchorQueryState targetState = getOrCompile(anchorSource, relation.getTargetAnchorType());
							for (SimpleMetricType<?> metric : targetState.theAllMetrics.theExposedMetrics) {
								state.theAllNonAgg.add(MetricTypeBuilder.buildRelated(relation, metric));
							}
							for (RelationMetricType<?> subRelation : targetState.theRelationMetrics.theExposedMetrics) {
								relations.add((RelationMetricType<?>) MetricTypeBuilder.buildRelated(relation, subRelation));
							}
						}
					}
				}
				return Collections.unmodifiableSet(state.theAllNonAgg);
			} finally {
				theLock.unlock();
			}
		}

		@Override
		public Set<BiTuple<MultiRelationMetricType<?, ?>, SimpleMetricType<?>>> getAllAggregate(AnchorSource anchorSource,
			AnchorType<?> anchorType) {
			theLock.lock();
			try {
				MetricAvailabilityAnchorQueryState state = getOrCompile(anchorSource, anchorType);
				if (state.theAllAgg.isEmpty()) {
					List<RelationMetricType<?>> relations = new ArrayList<>();
					List<RelationMetricType<?>> relationCopy = new ArrayList<>();
					List<MultiRelationMetricType<?, ?>> aggregates = new ArrayList<>();
					List<MultiRelationMetricType<?, ?>> aggregateCopy = new ArrayList<>();
					relations.addAll(state.theRelationMetrics.theExposedMetrics);
					aggregates.addAll(state.theMultiRelationMetrics.theExposedMetrics);
					for (int depth = 1; depth < RELATIVE_METRIC_DEPTH; depth++) {
						relationCopy.clear();
						relationCopy.addAll(relations);
						relations.clear();
						aggregateCopy.clear();
						aggregateCopy.addAll(aggregates);
						aggregates.clear();
						for (MultiRelationMetricType<?, ?> agg : aggregateCopy) {
							MetricAvailabilityAnchorQueryState targetState = getOrCompile(anchorSource, agg.getTargetAnchorType());
							for (SimpleMetricType<?> metric : targetState.theAllMetrics.theExposedMetrics) {
								state.theAllAgg.add(new BiTuple<>(agg, metric));
							}
							for (RelationMetricType<?> subRelation : targetState.theRelationMetrics.theExposedMetrics) {
								aggregates.add((MultiRelationMetricType<?, ?>) MetricTypeBuilder.buildAggregate(agg, subRelation, //
									StandardAggregateFunctions.distinct(subRelation.getType())));
							}
							for (MultiRelationMetricType<?, ?> subRelation : targetState.theMultiRelationMetrics.theExposedMetrics) {
								TypeToken<Anchor> relationAnchorType = (TypeToken<Anchor>) subRelation.getType()
									.resolveType(Collection.class.getTypeParameters()[0]);
								aggregates.add((MultiRelationMetricType<?, ?>) MetricTypeBuilder.buildAggregate(agg, //
									(MultiRelationMetricType<Anchor, ?>) subRelation, //
									StandardAggregateFunctions.flatten(relationAnchorType, true)));
							}
						}
						for (RelationMetricType<?> relation : relationCopy) {
							MetricAvailabilityAnchorQueryState targetState = getOrCompile(anchorSource, relation.getTargetAnchorType());
							for (RelationMetricType<?> subRelation : targetState.theRelationMetrics.theExposedMetrics) {
								relations.add((RelationMetricType<?>) MetricTypeBuilder.buildRelated(relation, subRelation));
							}
							for (MultiRelationMetricType<?, ?> subRelation : targetState.theMultiRelationMetrics.theExposedMetrics) {
								aggregates.add((MultiRelationMetricType<?, ?>) MetricTypeBuilder.buildRelated(relation, //
									(MultiRelationMetricType<Anchor, ?>) subRelation));
							}
						}
					}
				}
				return Collections.unmodifiableSet(state.theAllAgg);
			} finally {
				theLock.unlock();
			}
		}
	}

	static <T> Deque<T> emptyDeque() {
		class EmptyDeque<T2> implements Deque<T2> {
			@Override
			public boolean isEmpty() {
				return true;
			}

			@Override
			public Object[] toArray() {
				return new Object[0];
			}

			@Override
			public <T3> T3[] toArray(T3[] a) {
				return a;
			}

			@Override
			public boolean containsAll(Collection<?> c) {
				return c.isEmpty();
			}

			@Override
			public boolean addAll(Collection<? extends T2> c) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean removeAll(Collection<?> c) {
				return false;
			}

			@Override
			public boolean retainAll(Collection<?> c) {
				return false;
			}

			@Override
			public void clear() {}

			@Override
			public void addFirst(T2 e) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void addLast(T2 e) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean offerFirst(T2 e) {
				return false;
			}

			@Override
			public boolean offerLast(T2 e) {
				return false;
			}

			@Override
			public T2 removeFirst() {
				throw new NoSuchElementException();
			}

			@Override
			public T2 removeLast() {
				throw new NoSuchElementException();
			}

			@Override
			public T2 pollFirst() {
				throw new NoSuchElementException();
			}

			@Override
			public T2 pollLast() {
				throw new NoSuchElementException();
			}

			@Override
			public T2 getFirst() {
				throw new NoSuchElementException();
			}

			@Override
			public T2 getLast() {
				throw new NoSuchElementException();
			}

			@Override
			public T2 peekFirst() {
				return null;
			}

			@Override
			public T2 peekLast() {
				return null;
			}

			@Override
			public boolean removeFirstOccurrence(Object o) {
				return false;
			}

			@Override
			public boolean removeLastOccurrence(Object o) {
				return false;
			}

			@Override
			public boolean add(T2 e) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean offer(T2 e) {
				return false;
			}

			@Override
			public T2 remove() {
				throw new NoSuchElementException();
			}

			@Override
			public T2 poll() {
				throw new NoSuchElementException();
			}

			@Override
			public T2 element() {
				throw new NoSuchElementException();
			}

			@Override
			public T2 peek() {
				return null;
			}

			@Override
			public void push(T2 e) {
				throw new UnsupportedOperationException();
			}

			@Override
			public T2 pop() {
				throw new NoSuchElementException();
			}

			@Override
			public boolean remove(Object o) {
				return false;
			}

			@Override
			public boolean contains(Object o) {
				return false;
			}

			@Override
			public int size() {
				return 0;
			}

			@Override
			public Iterator<T2> iterator() {
				return new Iterator<T2>() {
					@Override
					public boolean hasNext() {
						return false;
					}

					@Override
					public T2 next() {
						throw new NoSuchElementException();
					}
				};
			}

			@Override
			public Iterator<T2> descendingIterator() {
				return iterator();
			}
		}
		return new EmptyDeque<>();
	}

	static <T> Deque<T> unmodifiableDeque(Deque<T> deque) {
		class UnmodifiableDeque<T2> implements Deque<T2> {
			private final Deque<T2> theDeque;

			UnmodifiableDeque(Deque<T2> innerDeque) {
				theDeque = innerDeque;
			}

			@Override
			public boolean isEmpty() {
				return theDeque.isEmpty();
			}

			@Override
			public Object[] toArray() {
				return theDeque.toArray();
			}

			@Override
			public <T3> T3[] toArray(T3[] a) {
				return theDeque.toArray(a);
			}

			@Override
			public boolean containsAll(Collection<?> c) {
				return theDeque.containsAll(c);
			}

			@Override
			public boolean addAll(Collection<? extends T2> c) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean removeAll(Collection<?> c) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean retainAll(Collection<?> c) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void clear() {
				throw new UnsupportedOperationException();
			}

			@Override
			public void addFirst(T2 e) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void addLast(T2 e) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean offerFirst(T2 e) {
				return false;
			}

			@Override
			public boolean offerLast(T2 e) {
				return false;
			}

			@Override
			public T2 removeFirst() {
				throw new UnsupportedOperationException();
			}

			@Override
			public T2 removeLast() {
				throw new UnsupportedOperationException();
			}

			@Override
			public T2 pollFirst() {
				throw new UnsupportedOperationException();
			}

			@Override
			public T2 pollLast() {
				throw new UnsupportedOperationException();
			}

			@Override
			public T2 getFirst() {
				return theDeque.getFirst();
			}

			@Override
			public T2 getLast() {
				return theDeque.getLast();
			}

			@Override
			public T2 peekFirst() {
				return theDeque.peekFirst();
			}

			@Override
			public T2 peekLast() {
				return theDeque.peekLast();
			}

			@Override
			public boolean removeFirstOccurrence(Object o) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean removeLastOccurrence(Object o) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean add(T2 e) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean offer(T2 e) {
				return false;
			}

			@Override
			public T2 remove() {
				throw new UnsupportedOperationException();
			}

			@Override
			public T2 poll() {
				throw new UnsupportedOperationException();
			}

			@Override
			public T2 element() {
				return theDeque.element();
			}

			@Override
			public T2 peek() {
				return theDeque.peek();
			}

			@Override
			public void push(T2 e) {
				throw new UnsupportedOperationException();
			}

			@Override
			public T2 pop() {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean remove(Object o) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean contains(Object o) {
				return theDeque.contains(o);
			}

			@Override
			public int size() {
				return theDeque.size();
			}

			@Override
			public Iterator<T2> iterator() {
				return IterableUtils.immutableIterator(theDeque.iterator());
			}

			@Override
			public Iterator<T2> descendingIterator() {
				return IterableUtils.immutableIterator(theDeque.descendingIterator());
			}
		}
		return new UnmodifiableDeque<>(deque);
	}
}
