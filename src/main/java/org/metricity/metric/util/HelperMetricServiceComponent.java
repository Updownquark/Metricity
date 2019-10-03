package org.metricity.metric.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.dbug.DBug;
import org.dbug.DBugAnchor;
import org.dbug.DBugAnchorType;
import org.metricity.anchor.Anchor;
import org.metricity.anchor.AnchorSource;
import org.metricity.anchor.AnchorType;
import org.metricity.anchor.types.WorldAnchor;
import org.metricity.metric.AggregateMetric;
import org.metricity.metric.Metric;
import org.metricity.metric.MetricSupport;
import org.metricity.metric.MetricType;
import org.metricity.metric.RelatedMetric;
import org.metricity.metric.SimpleMetric;
import org.metricity.metric.SimpleMetricType;
import org.metricity.metric.service.MetricChannel;
import org.metricity.metric.service.MetricChannelService;
import org.metricity.metric.service.MetricChannelServiceComponent;
import org.metricity.metric.service.MetricQueryResult;
import org.metricity.metric.service.MetricQueryService;
import org.metricity.metric.service.MetricTag;
import org.metricity.metric.service.TaggedMetricChannel;
import org.metricity.metric.util.derived.DerivedMetricChannelType;
import org.metricity.metric.util.derived.MappedChannelType;
import org.metricity.metric.util.derived.MetricDependencyType;
import org.metricity.metric.util.derived.MetricDependencyType.AnchorDefiner;
import org.metricity.metric.util.derived.TransformSource;
import org.observe.Observable;
import org.observe.ObservableValue;
import org.observe.SimpleObservable;
import org.observe.util.TypeTokens;
import org.qommons.BiTuple;
import org.qommons.LambdaUtils;
import org.qommons.TriFunction;
import org.qommons.ValueHolder;

import com.google.common.reflect.TypeToken;

/**
 * <p>
 * This abstract class makes implementing a {@link MetricChannelServiceComponent} to satisfy metrics.
 * </p>
 * 
 * <p>
 * Most implementations of this class need only implement the {@link #addMetrics()} and {@link #getPriority()} methods.
 * </p>
 * <p>
 * In the {@link #addMetrics()} method, the subclass will invoke one of the anchor source methods,
 * <ul>
 * <li>{@link #forAllAnchorSources()}</li>
 * <li>{@link #forAnchorSource(AnchorSource)} or</li>
 * <li>{@link #forAnchorSource(Predicate)}</li>
 * </ul>
 * to define what {@link Anchor#getSource() anchor source}s to satisfy metrics for. Components that satisfy metrics from configuration will
 * need to constrain the anchor source so that its metric satisfiers are not invoked for anchors that use different sources of
 * configuration. Components that satisfy metrics more generically as a function of other metric values may be able to satisfy metrics for
 * anchors of all sources (provided the dependencies of each metric are satisfied).
 * </p>
 * <p>
 * The forAnchorSource methods return a {@link ForAnchorSource} object, from which the component must define what {@link Anchor#getType()
 * types} of anchors to satisfy metrics for.
 * <ul>
 * <li>{@link ForAnchorSource#forAllAnchors()}</li>
 * <li>{@link ForAnchorSource#forAnchor(AnchorType...)}</li>
 * <li>{@link ForAnchorSource#forAnchors(Predicate)}</li>
 * <li>and others</li>
 * </ul>
 * Most metrics are satisfied for a specific anchor type or a small subset of anchor types, but some may be satisfied more generically for
 * any anchors for which the metric's dependencies are satisfied.
 * </p>
 * <p>
 * The forAnchor methods return a {@link ForAnchors} object, from which one or more metrics to be satisfied may be defined, in turn.
 * <ul>
 * <li>{@link ForAnchors#satisfy(SimpleMetric)}</li>
 * <li>{@link ForAnchors#satisfy(SimpleMetricType)}</li>
 * <li>{@link ForAnchors#satisfy(SimpleMetricType, Function)}</li>
 * </ul>
 * Only {@link SimpleMetric simple} metrics may be satisfied in this class, as {@link RelatedMetric related} and {@link AggregateMetric
 * aggregate} metrics are satisfied by the metric architecture as a function of the relation and relative metrics.
 * </p>
 * <p>
 * The satisfy methods that accept a function are filtered. The function should return null for metric instances that the satisfier can
 * indeed satisfy, otherwise a message indicating to the user what about the metric prevents the satisfier from satisfying it. For
 * debugging.
 * </p>
 * <p>
 * The satisfy methods return an instance of {@link ForMetric}. These classes have methods that allow the component to actually supply
 * values for the metric in one of several ways.
 * <ul>
 * <li><b>Constant</b> Supplies an immutable value for the metric.
 * <ul>
 * <li>{@link ForMetric#constant(BiFunction)}</li>
 * </ul>
 * </li>
 * <li><b>Observably</b> Supplies a value for the metric from an {@link ObservableValue}.
 * <ul>
 * <li>{@link ForMetric#statically(BiFunction)}</li>
 * </ul>
 * </li>
 * <li><b>Static</b> Supplies a value for the metric. Components that use this must use the {@link #getCache() cache}'s
 * {@link SimpleResultChannelCache#check(WorldAnchor, Object) check(WorldAnchor, Object)} method when a change in the world is detected to
 * trigger updates when the static changes.
 * <ul>
 * <li>{@link ForMetric#statically(Function, BiFunction)}</li>
 * <li>{@link ForMetric#statically(Function, BiFunction, Function)}</li>
 * </ul>
 * </li>
 * <li><b>Mapped</b> Invokes another anchor/metric that are both statically and immutably available from the target anchor/metric and
 * returns some simple transform that value.
 * <ul>
 * <li>{@link ForMetric#mapped(Function, double, Function)}</li>
 * </ul>
 * </li>
 * <li><b>Delegate</b> Invokes another anchor/metric that are both statically and constantly available from the target anchor/metric and
 * returns that value. Similar to <b>Mapped</b> but with no transformation.
 * <ul>
 * <li>{@link ForMetric#delegate(Function)}</li>
 * </ul>
 * </li>
 * <li><b>Derived</b> Declares a set of anchor/metric dependencies from whose values the target anchor/metric values may be derived.
 * <ul>
 * <li>{@link ForMetric#derive(Function)}</li>
 * </ul>
 * </li>
 * </ul>
 * In addition, the {@link ForMetric} class supports {@link ForMetric#filterAnchor(Function) anchor filtering} (which must be specified
 * prior to the satisfaction method call), in the case that some anchor instances, even of the same source and type, may not apply to the
 * satisfier. <i>This condition must rely only on immutable properties of the anchor.</i>
 * </p>
 */
public abstract class HelperMetricServiceComponent implements MetricChannelServiceComponent {
	/**
	 * A structure produced from one of the forAnchorSource methods:
	 * <ul>
	 * <li>{@link #forAllAnchorSources()}</li>
	 * <li>{@link #forAnchorSource(AnchorSource)} or</li>
	 * <li>{@link #forAnchorSource(Predicate)}</li>
	 * </ul>
	 * That allows the caller to define which anchor types to satisfy metrics for
	 */
	public interface ForAnchorSource {
		/**
		 * Tags all channels created with this anchor source with the given tag
		 * 
		 * @param category The category for the tag
		 * @param label The label (value) for the tag
		 * @return This ForAnchorSource utility structure
		 */
		ForAnchorSource tagAll(String category, String label);

		/**
		 * Call this to satisfy metrics regardless of anchor type
		 * 
		 * @return The structure to satisfy metrics in
		 */
		default ForAnchors<Anchor> forAllAnchors() {
			return forAnchors(LambdaUtils.printablePred(type -> true, "All Anchors", true));
		}

		/**
		 * Call this to satisfy metrics for a single anchor or a set of anchors with a common {@link Anchor} sub-type
		 * 
		 * @param <A> The {@link Anchor} sub-type of the anchors to satisfy metrics for
		 * @param anchorClass The {@link Anchor} sub-type of the anchors to satisfy metrics for
		 * @param anchorTypes The anchor types to satisfy metrics for
		 * @return The structure to satisfy metrics in
		 */
		default <A extends Anchor> ForAnchors<A> forAnchor(AnchorType<? extends A>... anchorTypes) {
			Set<AnchorType<? extends A>> set = new HashSet<>(anchorTypes.length);
			for (AnchorType<? extends A> at : anchorTypes) {
				set.add(at);
			}
			return (ForAnchors<A>) forAnchors(LambdaUtils.printablePred(set::contains, set.toString(), true));
		}

		/**
		 * Call this to satisfy metrics using a dynamic anchor type predicate
		 * 
		 * @param anchorType The test for whether a metric for an anchor's type is satisfiable in this structure
		 * @return The structure to satisfy metrics in
		 */
		ForAnchors<Anchor> forAnchors(Predicate<AnchorType<?>> anchorType);

		/**
		 * A shortcut to use to satisfy metrics for {@link WorldAnchor#ANCHOR_TYPE world}-type anchors
		 * 
		 * @return The structure to satisfy metrics in
		 */
		default ForAnchors<WorldAnchor> forWorlds() {
			return forAnchor(WorldAnchor.ANCHOR_TYPE);
		}
	}

	/**
	 * A structure to use to satisfy metrics for a set of anchors
	 *
	 * @param <A> The type of anchors that this structure may satisfy metrics for
	 */
	public interface ForAnchors<A extends Anchor> {
		/**
		 * Tags all channels created with this anchor source/type with the given tag
		 * 
		 * @param category The category for the tag
		 * @param label The label (value) for the tag
		 * @return This ForAnchorSource utility structure
		 */
		ForAnchors<A> tagAll(String category, String label);

		/**
		 * @param <T> The type of the metric
		 * @param metric The metric type to satisfy metrics for
		 * @return The structure to define how to satisfy metrics of the given type for anchors matching this anchor source/type definition
		 */
		default <T> ForMetric<A, T> satisfy(SimpleMetricType<T> metric) {
			return satisfy(metric, null);
		}

		/**
		 * @param <T> The type of the metric
		 * @param metric The metric to satisfy
		 * @return The structure to define how to satisfy the given metric for anchors matching this anchor source/type definition
		 */
		default <T> ForMetric<A, T> satisfy(SimpleMetric<T> metric) {
			Function<SimpleMetric<T>, String> test;
			if (metric.getType().getParameters().isEmpty()) {
				test = null;
			} else {
				test = LambdaUtils.printableFn(m -> metric.equals(m) ? null : "Only " + metric + " supported", metric::toString);
			}
			return satisfy(metric.getType(), test);
		}

		/**
		 * @param <T> The type of the metric
		 * @param metric The metric type to satisfy metrics for
		 * @param filter A function that returns null if the argument is satisfiable by the returned structure, or a message describing why
		 *        it isn't (for debugging)
		 * @return The structure to define how to satisfy metrics of the given type for anchors matching this anchor source/type definition
		 */
		<T> ForMetric<A, T> satisfy(SimpleMetricType<T> metric, Function<? super SimpleMetric<T>, String> filter);

		/**
		 * For re-using the {@link ForAnchorSource} that generated this anchor structure inline (chaining)
		 * 
		 * @return The {@link ForAnchorSource} that generated this anchor structure
		 */
		ForAnchorSource out();
	}

	/**
	 * A structure to define satisfaction for a {@link MetricType}
	 *
	 * @param <A> The type of anchor to satisfy the metric for
	 * @param <T> The value type of the metric to satisfy
	 */
	public interface AbstractForMetric<A extends Anchor, T> {
		/**
		 * Use this to satisfy the metric only for anchors with certain static, immutable properties
		 * 
		 * @param filter The filter to determine whether this satisfier can be used to satisfy the metric for an anchor
		 * @return This utility structure
		 */
		AbstractForMetric<A, T> filterAnchor(Function<A, String> filter);

		/**
		 * Adds a tag to metric channels generated by this satisfier
		 * 
		 * @param category The category of the tag
		 * @param label The label (value) of the tag
		 * @return This utility structure
		 */
		AbstractForMetric<A, T> tag(String category, String label);

		/**
		 * Satisfies the metric by transformation of the value of a different anchor/metric pair that is statically and immutably accessible
		 * from the target anchor/metric.
		 * 
		 * @param <X> The type of the delegate metric
		 * @param dependency Defines an anchor/metric for each target anchor/metric to use to derive the target metric value
		 * @param cost The cost of the mapping transformation
		 * @param map The function to produce a target metric value from the delegated metric value
		 * @return The anchor structure to use to satisfy more metrics
		 */
		<X> ForAnchors<A> mapped(Function<MetricDependencyType.AnchorDefiner<A, T>, MetricDependencyType<T, X>> dependency, double cost,
			Function<TransformSource<A, X, T>, T> map);
	}

	/**
	 * A structure to define satisfaction for a {@link MetricType}
	 *
	 * @param <A> The type of anchor to satisfy the metric for
	 * @param <T> The value type of the metric to satisfy
	 */
	public interface ForMetric<A extends Anchor, T> extends AbstractForMetric<A, T> {
		@Override
		ForMetric<A, T> filterAnchor(Function<A, String> filter);

		@Override
		ForMetric<A, T> tag(String category, String label);

		/**
		 * Creates a channel whose query results are time-independent and immutable. If an attribute is time-variable or mutable, using this
		 * will cause problems.
		 * 
		 * @param value A function to produce the constant value for an anchor
		 * @return The anchor satisfaction builder to satisfy more metrics
		 */
		ForAnchors<A> constant(BiFunction<? super A, Metric<T>, ? extends T> value);

		/**
		 * <p>
		 * Satisfies the metric with a static, observable value--a value that is not time-sensitive, but may be mutable
		 * </p>
		 * <p>
		 * Unlike the other <code>statically</code> methods, this method does not require interaction with the
		 * {@link HelperMetricServiceComponent#getCache() cache}, since its updates come via the ObServe API.
		 * </p>
		 * 
		 * @param value The function to produce a value for an anchor/metric pair
		 * @return The anchor satisfaction builder to satisfy more metrics
		 */
		ForAnchors<A> statically(BiFunction<? super A, Metric<T>, ? extends ObservableValue<? extends T>> value);

		/**
		 * <p>
		 * Satisfies the metric with a static value--a value that is not time-sensitive, but may be mutable
		 * </p>
		 * <p>
		 * Components that use this MUST listen for change events and pipe them to the {@link HelperMetricServiceComponent#getCache()
		 * channel Cache} (see {@link SimpleResultChannelCache#check(WorldAnchor, Object)})
		 * </p>
		 * 
		 * @param <AO> The anchor object type to operate on (simplifies retrieval of representative values for an anchor)
		 * @param anchorObject Produces an anchor object from the raw anchor
		 * @param value Produces the metric value from the anchor object and metric
		 * @return The anchor satisfaction builder to satisfy more metrics for the anchor
		 */
		default <AO> ForAnchors<A> statically(Function<A, AO> anchorObject, BiFunction<? super AO, Metric<T>, ? extends T> value) {
			return statically(anchorObject, value, v -> v);
		}

		/**
		 * <p>
		 * Satisfies the metric with a static value--a value that is not time-sensitive, but may be mutable
		 * </p>
		 * <p>
		 * Components that use this MUST listen for change events and pipe them to the {@link HelperMetricServiceComponent#getCache()
		 * channel Cache} (see {@link SimpleResultChannelCache#check(WorldAnchor, Object)})
		 * </p>
		 * 
		 * @param <AO> The anchor object type to operate on (simplifies retrieval of representative values for an anchor)
		 * @param anchorObject Produces an anchor object from the raw anchor
		 * @param value Produces the metric value from the anchor object and metric
		 * @param copy The copy function to produce an independent copy. This is needed to detect when the value has changed within the
		 *        {@link HelperMetricServiceComponent#getCache() cache}'s {@link SimpleResultChannelCache#check(WorldAnchor, Object)
		 *        check(WorldAnchor)} method.
		 * @return The anchor satisfaction builder to satisfy more metrics for the anchor
		 */
		<AO> ForAnchors<A> statically(Function<A, AO> anchorObject, BiFunction<? super AO, Metric<T>, ? extends T> value,
			Function<T, T> copy);

		/**
		 * Satisfies the metric using the value of a statically, immutably available from the target anchor/metric. Same as
		 * {@link #mapped(Function, double, Function)}, but with no transformation.
		 * 
		 * @param dependency Defines an anchor/metric for each target anchor/metric to satisfy the target metric value
		 * @return The anchor satisfaction builder to satisfy more metrics for the anchor
		 */
		default ForAnchors<A> delegate(Function<MetricDependencyType.AnchorDefiner<A, T>, MetricDependencyType<T, T>> dependency) {
			return mapped(dependency, 0, null);
		}

		/**
		 * Satisfies the metric as a function of one or more other declared anchor/metric dependencies.
		 * 
		 * @param build Defines the dependencies and the function to combine them to produce this metric's values
		 * @return The anchor satisfaction builder to satisfy more metrics for the anchor
		 */
		ForAnchors<A> derive(Function<DerivedMetricChannelType.Builder<A, T>, DerivedMetricChannelType<T>> build);
	}

	private static final DBugAnchorType<HelperMetricServiceComponent> DEBUG_TYPE = DBug.declare("metricity",
		HelperMetricServiceComponent.class, b -> b//
			.withEvent("loaded", null)//
			.withEvent("getChannelFail", eb -> eb//
				.withEventField("anchor", TypeTokens.get().of(Anchor.class))//
				.withEventField("metric", new TypeToken<Metric<?>>() {})//
				.withEventField("satisfier", new TypeToken<MetricSatisfier<?>>() {})//
				.withEventField("reason", TypeTokens.get().STRING)//
			)//
	);
	private final DBugAnchor<HelperMetricServiceComponent> debug = DEBUG_TYPE.debug(this).build();

	private final Map<SimpleMetricType<?>, List<MetricSatisfier<?>>> theMetrics;
	private final SimpleObservable<Void> theChanges;
	private SimpleResultChannelCache theCache;
	private Map<BiTuple<AnchorSource, AnchorType<?>>, List<String>> theMetricFailures;
	private boolean isLoaded;

	/** Creates the component */
	public HelperMetricServiceComponent() {
		theMetrics = new LinkedHashMap<>();
		theChanges = new SimpleObservable<>();
	}

	/**
	 * For components that satisfy metrics in {@link ForMetric#statically(Function, BiFunction, Function) static} ways, the world must be
	 * monitored for changes and the {@link SimpleResultChannelCache#check(WorldAnchor, Object) cache's check(WorldAnchor)} method must be
	 * called when the world may have changed. That method determines which static or state-based metrics have actually changed and fires
	 * appropriate events.
	 * 
	 * @return The simple result cache for this component
	 */
	protected SimpleResultChannelCache getCache() {
		if (theCache == null) {
			theCache = new SimpleResultChannelCache();
		}
		return theCache;
	}

	/** Adds metric satisfiers to this component */
	protected abstract void addMetrics();

	/** Clears all metric satisfiers from this component */
	protected void clearSupport() {
		theMetrics.clear();
		if (theCache != null) {
			theCache.clear();
		}
		supportChanged();
	}

	/**
	 * If a component's metric support is not constant, this method must be invoked when it changes. The satisfaction methods do not do this
	 * by themselves.
	 */
	protected void supportChanged() {
		theChanges.onNext(null);
	}

	/**
	 * When it is not clear why a metric is not being satisfied, invoking this method at initialization will cause print outs that may help
	 * determine the reason.
	 */
	protected void debug() {
		if (theMetricFailures != null) {
			return;
		}
		theMetricFailures = new HashMap<>();
		new Thread(() -> {
			try { // Wait for all dependencies to be loaded
				Thread.sleep(20000);
			} catch (InterruptedException e) {}
			Map<BiTuple<AnchorSource, AnchorType<?>>, List<String>> failures = theMetricFailures;
			theMetricFailures = null;
			boolean failed = false;
			for (Entry<BiTuple<AnchorSource, AnchorType<?>>, List<String>> failure : failures.entrySet()) {
				for (String msg : failure.getValue()) {
					failed = true;
					System.err.println(getClass().getSimpleName() + ": " + failure.getKey() + ": " + msg);
				}
			}
			if (!failed) {
				System.out.println(getClass().getSimpleName() + ": No failures");
			}
		}, getClass().getName() + " Debugger").start();
	}

	/**
	 * Call this from {@link #addMetrics()} to begin satisfying metrics for anchors using a source filter
	 * 
	 * @param anchorSource A filter that returns true when an anchor's source may allow metrics for it to be satisfied by the
	 *        {@link ForAnchorSource}'s satisfiers
	 * @return The {@link ForAnchorSource} to define anchor inclusion in
	 */
	protected ForAnchorSource forAnchorSource(Predicate<AnchorSource> anchorSource) {
		return new ForAnchorSourceImpl(anchorSource);
	}

	/**
	 * Call this from {@link #addMetrics()} to begin satisfying metrics for anchors for a specific source
	 * 
	 * @param anchorSource The anchor source of anchors whose metrics that may be satisfied by the {@link ForAnchorSource}'s satisfiers
	 * @return The {@link ForAnchorSource} to define anchor inclusion in
	 */
	protected ForAnchorSource forAnchorSource(AnchorSource anchorSource) {
		return forAnchorSource(//
			LambdaUtils.printablePred(src -> anchorSource.equals(src), anchorSource.getName(), true));
	}

	/**
	 * Call this from {@link #addMetrics()} to begin satisfying metrics for all anchor sources
	 * 
	 * @return The {@link ForAnchorSource} to define anchor inclusion in
	 */
	protected ForAnchorSource forAllAnchorSources() {
		return forAnchorSource(//
			LambdaUtils.printablePred(src -> true, "All Anchor Sources", true));
	}

	@Override
	public void initialize(MetricQueryService depends) {
		if (!isLoaded) {
			isLoaded = true;
			debug.event("loaded").occurred();
		}
		addMetrics();
	}

	@Override
	public Set<SimpleMetricType<?>> getAllSupportedMetrics(AnchorSource anchorSource, AnchorType<?> anchorType, MetricSupport support) {
		List<String> failures = theMetricFailures == null ? null
			: theMetricFailures.computeIfAbsent(new BiTuple<>(anchorSource, anchorType), k -> new ArrayList<>());
		if (failures != null) {
			failures.clear();
		}
		Set<SimpleMetricType<?>> metrics = new HashSet<>();
		for (List<MetricSatisfier<?>> metric : theMetrics.values()) {
			for (MetricSatisfier<?> derivation : metric) {
				String err = derivation.applies(anchorSource, anchorType);
				if (err != null) {
					continue;
				}
				String msg = derivation.isSatisfied(anchorSource, anchorType, support);
				if (msg == null) {
					metrics.add(derivation.getMetric());
				} else if (failures != null) {
					failures.add(derivation + ": " + msg);
				}
			}
		}
		return metrics;
	}

	@Override
	public Observable<?> supportChanges() {
		return theChanges;
	}

	@Override
	public <T> List<? extends MetricChannel<T>> getChannels(Anchor anchor, Metric<T> metric, MetricChannelService dependencies,
		Consumer<String> onError) {
		List<MetricSatisfier<T>> satisfiers = (List<MetricSatisfier<T>>) (List<?>) theMetrics.get(metric.getType());
		if (satisfiers == null) {
			return Collections.emptyList();
		}
		List<MetricChannel<T>> channels = new ArrayList<>(satisfiers.size());
		ValueHolder<String> reason = (debug.isActive() || onError != null) ? new ValueHolder<>() : null;
		for (MetricSatisfier<T> satisfier : satisfiers) {
			String err = satisfier.isSatisfied(anchor.getSource(), anchor.getType(), null);
			if (reason != null) {
				reason.accept(err);
			}
			if (err == null) {
				MetricChannel<T> channel = satisfier.createChannel(anchor, metric, dependencies, reason);
				if (channel != null) {
					channels.add(channel);
				} else {
					debug.event("getChannelFail").with("anchor", anchor).with("metric", metric).with("satisfier", satisfier)
						.with("reason", reason::get).occurred();
				}
			} else {
				debug.event("getChannelFail").with("anchor", anchor).with("metric", metric).with("satisfier", satisfier)
					.with("reason", reason::get).occurred();
			}
		}
		if (channels.isEmpty() && onError != null && reason.get() != null) {
			onError.accept(reason.get());
		}
		return Collections.unmodifiableList(channels);
	}

	@Override
	public <T> MetricChannel<T> getChannel(Anchor anchor, Metric<T> metric, MetricChannelService dependencies, Consumer<String> onError) {
		List<MetricSatisfier<T>> satisfiers = (List<MetricSatisfier<T>>) (List<?>) theMetrics.get(metric.getType());
		if (satisfiers == null) {
			return null;
		}
		StringAppendingErrorHandler reason = (debug.isActive() || onError != null) ? new StringAppendingErrorHandler(", ") : null;
		for (MetricSatisfier<T> satisfier : satisfiers) {
			StringAppendingErrorHandler sReason = reason == null ? null : new StringAppendingErrorHandler(", ");
			String err = satisfier.isSatisfied(anchor.getSource(), anchor.getType(), null);
			if (sReason != null) {
				sReason.accept(err);
			}
			if (err == null) {
				MetricChannel<T> channel = satisfier.createChannel(anchor, metric, dependencies, sReason == null ? null : e -> {
					sReason.accept(e);
					reason.accept(e);
				});
				if (channel != null) {
					return channel;
				} else {
					debug.event("getChannelFail").with("anchor", anchor).with("metric", metric).with("satisfier", satisfier)
						.with("reason", sReason::toString).occurred();
				}
			} else {
				if (reason != null) {
					reason.accept(sReason.toString());
				}
				debug.event("getChannelFail").with("anchor", anchor).with("metric", metric).with("satisfier", satisfier)
					.with("reason", sReason::toString).occurred();
			}
		}
		if (onError != null && !reason.isEmpty()) {
			onError.accept(reason.borderWith("{", "}").toString());
		}
		return null;
	}

	@Override
	public double getCost(MetricChannel<?> channel) {
		if (channel instanceof ConstantChannel) {
			return 0;
		} else if (channel instanceof SimpleResultMetricChannel) {
			return ((SimpleResultMetricChannel<?>) channel).getCost();
		} else {
			throw new IllegalStateException("Unrecognized channel: " + channel);
		}
	}

	@Override
	public Set<MetricTag> getTags(MetricChannel<?> channel) {
		if (channel instanceof TaggedMetricChannel) {
			return ((TaggedMetricChannel<?>) channel).getTags();
		} else {
			throw new IllegalStateException("Unrecognized channel: " + channel);
		}
	}

	@Override
	public boolean isConstant(MetricChannel<?> channel) {
		if (channel instanceof ConstantChannel)
			return true;
		else if (channel instanceof SimpleResultMetricChannel)
			return ((SimpleResultMetricChannel<?>) channel).isConstant();
		else
			throw new IllegalStateException("Unrecognized channel: " + channel);
	}

	@Override
	public <T> MetricQueryResult<T> query(MetricChannel<T> channel, Consumer<Object> initialListener) {
		if (channel instanceof ConstantChannel) {
			return MetricQueryResult.constant(channel, ((ConstantChannel<T>) channel).getCurrentValue());
		} else if (channel instanceof SimpleResultMetricChannel) {
			return ((SimpleResultMetricChannel<T>) channel).query(initialListener);
		} else {
			throw new IllegalStateException("Unrecognized channel: " + channel);
		}
	}

	private class ForAnchorSourceImpl implements ForAnchorSource {
		private final Predicate<AnchorSource> theAnchorSource;
		private Set<MetricTag> theTags;

		ForAnchorSourceImpl(Predicate<AnchorSource> anchorSource) {
			theAnchorSource = anchorSource;
		}

		@Override
		public ForAnchorSource tagAll(String category, String label) {
			if (theTags == null) {
				theTags = new TreeSet<>();
			}
			theTags.add(new MetricTagImpl(category, label));
			return this;
		}

		@Override
		public ForAnchors<Anchor> forAnchors(Predicate<AnchorType<?>> anchorType) {
			return new ForAnchorsImpl(this, anchorType, theTags);
		}

		@Override
		public String toString() {
			return theAnchorSource.toString();
		}
	}

	private class ForAnchorsImpl implements ForAnchors<Anchor> {
		private final ForAnchorSourceImpl theAnchorSource;
		private final Predicate<AnchorType<?>> theAnchorType;
		private Set<MetricTag> theTags;

		ForAnchorsImpl(ForAnchorSourceImpl anchorSource, Predicate<AnchorType<?>> anchorType, Set<MetricTag> tags) {
			theAnchorSource = anchorSource;
			theAnchorType = anchorType;
			theTags = tags == null ? null : new TreeSet<>(tags);
		}

		@Override
		public ForAnchors<Anchor> tagAll(String category, String label) {
			if (theTags == null) {
				theTags = new TreeSet<>();
			}
			theTags.add(new MetricTagImpl(category, label));
			return this;
		}

		@Override
		public <T> ForMetric<Anchor, T> satisfy(SimpleMetricType<T> metric, Function<? super SimpleMetric<T>, String> filter) {
			return new ForMetricImpl<>(this, metric, filter, theTags);
		}

		@Override
		public ForAnchorSource out() {
			return theAnchorSource;
		}

		@Override
		public String toString() {
			return theAnchorSource + ", " + theAnchorType;
		}
	}

	private <T> void addProducer(ForAnchorsImpl anchors, SimpleMetricType<T> metricType, Function<Anchor, String> anchorFilter,
		Function<? super SimpleMetric<T>, String> metricFilter,
		TriFunction<Anchor, Metric<T>, Consumer<String>, MetricChannel<T>> channel) {
		theMetrics.computeIfAbsent(metricType, m -> new LinkedList<>()).add(new MetricSatisfier<T>() {
			@Override
			public SimpleMetricType<T> getMetric() {
				return metricType;
			}

			@Override
			public String applies(AnchorSource anchorSource, AnchorType<?> anchorType) {
				if (!anchors.theAnchorSource.theAnchorSource.test(anchorSource)) {
					return new StringBuilder("Anchor source ").append(anchorSource).append(" does not apply to ")
						.append(anchors.theAnchorSource.theAnchorSource).toString();
				} else if (!anchors.theAnchorType.test(anchorType)) {
					return new StringBuilder("Anchor type ").append(anchorType).append(" does not apply to ").append(anchors.theAnchorType)
						.toString();
				} else {
					return null;
				}
			}

			@Override
			public String isSatisfied(AnchorSource anchorSource, AnchorType<?> anchorType, MetricSupport support) {
				return applies(anchorSource, anchorType);
			}

			@Override
			public MetricChannel<T> createChannel(Anchor anchor, Metric<T> metric, MetricChannelService depends, Consumer<String> onError) {
				String err = null;
				if (anchorFilter != null) {
					err = anchorFilter.apply(anchor);
				}
				if (err == null && metricFilter != null) {
					err = metricFilter.apply((SimpleMetric<T>) metric);
				}
				if (err != null) {
					if (onError != null) {
						onError.accept(err);
					}
					return null;
				}
				return channel.apply(anchor, metric, onError);
			}

			@Override
			public String toString() {
				return metricType.getDisplayName() + " (" + anchors + ")";
			}
		});
		// Probably nobody's listening because this is being called by initialization before anybody has a reference to us,
		// but if for any reason any implementations decide to support a metric in the middle of things, might as well handle it
		theChanges.onNext(null);
	}

	private abstract class AbstractForMetricImpl<T> implements AbstractForMetric<Anchor, T> {
		protected Function<Anchor, String> theAnchorFilter;
		protected final ForAnchorsImpl theAnchors;
		protected final SimpleMetricType<T> theMetric;
		protected final Function<? super SimpleMetric<T>, String> theMetricFilter;
		protected Set<MetricTag> theTags;

		AbstractForMetricImpl(ForAnchorsImpl anchors, SimpleMetricType<T> metric, Function<? super SimpleMetric<T>, String> filter,
			Set<MetricTag> tags) {
			theAnchors = anchors;
			theMetric = metric;
			theMetricFilter = filter;
			theTags = tags == null ? null : new TreeSet<>(tags);
		}

		@Override
		public AbstractForMetric<Anchor, T> filterAnchor(Function<Anchor, String> filter) {
			if (theAnchorFilter == null) {
				theAnchorFilter = filter;
			} else {
				Function<Anchor, String> oldFilter = theAnchorFilter;
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

		@Override
		public AbstractForMetric<Anchor, T> tag(String category, String label) {
			if (theTags == null) {
				theTags = new TreeSet<>();
			}
			theTags.add(new MetricTagImpl(category, label));
			return this;
		}

		@Override
		public <X> ForAnchors<Anchor> mapped(Function<AnchorDefiner<Anchor, T>, MetricDependencyType<T, X>> dependency, double cost,
			Function<TransformSource<Anchor, X, T>, T> map) {
			MetricDependencyType.AnchorDefiner<Anchor, T> ad = new MetricDependencyType.AnchorDefiner<>("only", false);
			MetricDependencyType<T, X> depend = dependency.apply(ad);
			if (depend == null) {
				return theAnchors;
			}

			MappedChannelType<X, T> channelType = new MappedChannelType<X, T>(theMetric, //
				theAnchors.theAnchorSource.theAnchorSource, theAnchors.theAnchorType, //
				theAnchorFilter, theMetricFilter,
				theTags == null ? Collections.emptyNavigableSet() : Collections.unmodifiableNavigableSet(new TreeSet<>(theTags)), depend,
				map, cost);
			theMetrics.computeIfAbsent(theMetric, m -> new LinkedList<>()).add(channelType);
			// Probably nobody's listening because this is being called by initialization before anybody has a reference to us,
			// but if for any reason any implementations decide to support a metric in the middle of things, might as well handle it
			theChanges.onNext(null);
			return theAnchors;
		}
	}

	private class ForMetricImpl<T> extends AbstractForMetricImpl<T> implements ForMetric<Anchor, T> {
		ForMetricImpl(ForAnchorsImpl anchors, SimpleMetricType<T> metric, Function<? super SimpleMetric<T>, String> filter,
			Set<MetricTag> tags) {
			super(anchors, metric, filter, tags);
		}

		@Override
		public ForMetric<Anchor, T> filterAnchor(Function<Anchor, String> filter) {
			super.filterAnchor(filter);
			return this;
		}

		@Override
		public ForMetric<Anchor, T> tag(String category, String label) {
			super.tag(category, label);
			return this;
		}

		@Override
		public ForAnchors<Anchor> constant(BiFunction<? super Anchor, Metric<T>, ? extends T> value) {
			addProducer(theAnchors, theMetric, theAnchorFilter, theMetricFilter,
				(a, m, e) -> ConstantChannel.create(HelperMetricServiceComponent.this, a, m, theTags, value.apply(a, m)));
			return theAnchors;
		}

		@Override
		public ForAnchors<Anchor> statically(BiFunction<? super Anchor, Metric<T>, ? extends ObservableValue<? extends T>> value) {
			addProducer(theAnchors, theMetric, theAnchorFilter, theMetricFilter, //
				(a, m, e) -> ObservableChannel.create(a, m, value.apply(a, m), theTags));
			return theAnchors;
		}

		@Override
		public <AO> ForAnchors<Anchor> statically(Function<Anchor, AO> anchorObject, BiFunction<? super AO, Metric<T>, ? extends T> value,
			Function<T, T> copy) {
			addProducer(theAnchors, theMetric, theAnchorFilter, theMetricFilter,
				(a, m, e) -> StaticChannel.create(getCache(), HelperMetricServiceComponent.this, a, m, theTags, anchorObject.apply(a), //
					ao -> value.apply(ao, m), copy));
			return theAnchors;
		}

		@Override
		public ForAnchors<Anchor> derive(Function<DerivedMetricChannelType.Builder<Anchor, T>, DerivedMetricChannelType<T>> build) {
			DerivedMetricChannelType.Builder<Anchor, T> builder = DerivedMetricChannelType.build(theMetric,
				theAnchors.theAnchorSource.theAnchorSource, theAnchors.theAnchorType, theMetricFilter);
			if (theTags != null) {
				for (MetricTag tag : theTags) {
					builder.withTag(tag.getCategory(), tag.getLabel());
				}
			}
			if (theAnchorFilter != null) {
				builder.filter(theAnchorFilter);
			}
			DerivedMetricChannelType<T> channelType = build.apply(builder);
			theMetrics.computeIfAbsent(theMetric, m -> new LinkedList<>()).add(channelType);
			// Probably nobody's listening because this is being called by initialization before anybody has a reference to us,
			// but if for any reason any implementations decide to support a metric in the middle of things, might as well handle it
			theChanges.onNext(null);
			return theAnchors;
		}
	}
}
