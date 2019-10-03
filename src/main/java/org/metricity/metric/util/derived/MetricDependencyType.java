package org.metricity.metric.util.derived;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.metricity.anchor.Anchor;
import org.metricity.anchor.AnchorType;
import org.metricity.anchor.types.WorldAnchor;
import org.metricity.metric.Metric;
import org.metricity.metric.MetricType;
import org.metricity.metric.service.MetricChannel;
import org.metricity.metric.service.MetricQueryService;
import org.qommons.LambdaUtils;

/**
 * The definition of a dependency of a target anchor/metric on an anchor/metric whose value is needed to resolve the target metric value
 * 
 * @param <T> The target metric value
 * @param <X> The dependency metric value
 */
public class MetricDependencyType<T, X> {
	private final String theName;
	private final MetricType<X> theMetric;
	private final BiFunction<Anchor, Metric<T>, ? extends Metric<X>> theMetricProducer;
	private final Function<? extends AnchorType<?>, ? extends AnchorType<?>> theAnchorType;
	private final BiFunction<Anchor, Metric<T>, ? extends Anchor> theAnchorProducer;
	private final Function<? super DependencyTypeSet, DependencyType> isRequired;
	private final Function<? super DependencyInstanceSet<Anchor, T>, DependencyType> isInstanceRequired;
	private final Function<? super DependencyValueSet<Anchor, T>, ? extends List<? extends Metric<X>>> theDynamicMetricProducer;
	private final Function<? super DependencyValueSet<Anchor, T>, ? extends List<? extends Anchor>> theDynamicAnchorProducer;
	private final Function<? super DependencyValueSet<Anchor, T>, DependencyType> isDynamicallyRequired;
	final ConditionalChannelFilter<Anchor, T, X> theChannelFilter;
	private final boolean isAdaptive;
	private final boolean isMulti;

	MetricDependencyType(String name, //
		MetricType<X> metricType, BiFunction<Anchor, Metric<T>, ? extends Metric<X>> metric, //
		Function<? extends AnchorType<?>, ? extends AnchorType<?>> anchorType, BiFunction<Anchor, Metric<T>, ? extends Anchor> anchor, //
		Function<? super DependencyTypeSet, DependencyType> required,
		Function<? super DependencyInstanceSet<Anchor, T>, DependencyType> instanceRequired,
		Function<? super DependencyValueSet<Anchor, T>, ? extends List<? extends Anchor>> dynamicAnchor,
		Function<? super DependencyValueSet<Anchor, T>, ? extends List<? extends Metric<X>>> dynamicMetric,
		Function<? super DependencyValueSet<Anchor, T>, DependencyType> dynamicRequired,
		ConditionalChannelFilter<Anchor, T, X> channelFilter, boolean adaptive, boolean multi) {
		theName = name;
		theMetric = metricType;
		theMetricProducer = metric;
		theAnchorType = anchorType;
		theAnchorProducer = anchor;
		isRequired = required;
		isInstanceRequired = instanceRequired;
		theDynamicAnchorProducer = dynamicAnchor;
		theDynamicMetricProducer = dynamicMetric;
		isDynamicallyRequired = dynamicRequired;
		theChannelFilter = channelFilter;
		isAdaptive = adaptive;
		isMulti = multi;
	}

	/** @return The name of the dependency */
	public String getName() {
		return theName;
	}

	/** @return The metric type of the dependency */
	public MetricType<X> getMetric() {
		return theMetric;
	}

	/**
	 * @param mainAnchorType The target anchor type
	 * @return The anchor type of the dependency
	 */
	public AnchorType<?> getAnchorType(AnchorType<?> mainAnchorType) {
		return ((Function<AnchorType<?>, AnchorType<?>>) theAnchorType).apply(mainAnchorType);
	}

	/**
	 * @param mainAnchor The target anchor
	 * @param mainMetric The target metric
	 * @return The dependency anchor
	 */
	protected Anchor getAnchor(Anchor mainAnchor, Metric<T> mainMetric) {
		return theAnchorProducer.apply(mainAnchor, mainMetric);
	}

	/**
	 * @param mainAnchor The target anchor
	 * @param mainMetric The target metric
	 * @return The dependency metric
	 */
	protected Metric<X> getMetric(Anchor mainAnchor, Metric<T> mainMetric) {
		return theMetricProducer.apply(mainAnchor, mainMetric);
	}

	/**
	 * @param ds Dependency information
	 * @return The needed-level of the dependency given what <code>ds</code> can tell us
	 */
	public DependencyType isRequired(DependencyTypeSet ds) {
		return isRequired.apply(ds);
	}

	/**
	 * @param ds Dependency information
	 * @return The needed-level of the dependency in this situation
	 */
	public DependencyType isRequiredForInstance(DependencyInstanceSet<Anchor, T> ds) {
		return isInstanceRequired.apply(ds);
	}

	/** @return Whether this dependency is {@link DerivedMetricChannelType.Builder#adapt(double, Function, Function) adaptive} */
	public boolean isAdaptive() {
		return isAdaptive;
	}

	/** @return Whether this dependency is {@link DerivedMetricChannelType.Builder#dependsDynamic(String, Function) dynamic} */
	public boolean isDynamic() {
		return theMetricProducer == null;
	}

	/** @return Whether this dependency type represents multiple dependency channels */
	public boolean isMulti() {
		return isMulti;
	}

	/**
	 * @param ds The static dependency values
	 * @return The anchors for this dependency
	 */
	public List<? extends Anchor> getAnchor(DependencyValueSet<Anchor, T> ds) {
		if (theDynamicAnchorProducer == null) {
			return Arrays.asList(theAnchorProducer.apply(ds.getAnchor(), ds.getMetric()));
		} else {
			return theDynamicAnchorProducer.apply(ds);
		}
	}

	/**
	 * @param ds The static dependency values
	 * @return The metrics for this dependency
	 */
	public List<? extends Metric<X>> getMetric(DependencyValueSet<Anchor, T> ds) {
		if (theDynamicMetricProducer == null) {
			return Arrays.asList(theMetricProducer.apply(ds.getAnchor(), ds.getMetric()));
		} else {
			return theDynamicMetricProducer.apply(ds);
		}
	}

	/**
	 * @param ds The static dependency values
	 * @return The needfulness of this dependency for the target
	 */
	public DependencyType isDynamicallyRequired(DependencyValueSet<Anchor, T> ds) {
		if (isDynamicallyRequired == null) {
			return DependencyType.Required;
		}
		return isDynamicallyRequired.apply(ds);
	}

	/** @return Whether this dependency filters channels instead of just taking the highest-priority one */
	public boolean hasChannelFilter() {
		return theChannelFilter != null;
	}

	/**
	 * @param channels The available channels for this dependency
	 * @param ds Dependency information
	 * @param source The query service
	 * @return The channel to use for this dependency
	 */
	public MetricChannel<X> filterChannels(List<? extends MetricChannel<X>> channels, DependencyInstanceSet<Anchor, T> ds,
		MetricQueryService source) {
		return theChannelFilter.filterFor(ds).selectFrom(channels, source);
	}

	/**
	 * @param ds The static dependency values
	 * @return The channel filter to use for this dependency
	 */
	public ChannelFilter<Anchor, X> filterForValues(DependencyValueSet<Anchor, T> ds) {
		return theChannelFilter.filterFor(ds);
	}

	@Override
	public String toString() {
		return theName + ": " + theAnchorType + "." + theAnchorProducer + "."
			+ (theMetric == null ? "Dynamic metric" : theMetric.getDisplayName()) + "(" + theMetricProducer + ") " + isRequired + " ("
			+ isInstanceRequired + ")";
	}

	/**
	 * Initial step for defining a static dependency--defining the dependency anchor
	 * 
	 * @param <ASrc> The type of the target anchor
	 * @param <T> The type of the target metric
	 */
	public static class AnchorDefiner<ASrc extends Anchor, T> {
		final String theDependencyName;
		final boolean isAdaptive;

		/**
		 * @param dependencyName The name of the dependency
		 * @param adaptive Whether the dependency is adaptive
		 */
		public AnchorDefiner(String dependencyName, boolean adaptive) {
			theDependencyName = dependencyName;
			isAdaptive = adaptive;
		}

		/**
		 * Use this if the anchor for the dependency is the target anchor
		 * 
		 * @return The metric definition precursor
		 */
		public MetricDefiner<ASrc, ASrc, T> self() {
			return anchor(LambdaUtils.identity(), LambdaUtils.printableBiFn((a, m) -> a, "self", true));
		}

		/**
		 * Use this if the anchor for the dependency is the target anchor's world
		 * 
		 * @return The metric definition precursor
		 */
		public MetricDefiner<ASrc, WorldAnchor, T> world() {
			return anchor(WorldAnchor.ANCHOR_TYPE, LambdaUtils.printableFn(Anchor::getWorld, "world", true));
		}

		/**
		 * Use this if the type of the dependency anchor is constant, but the anchor is a function of the target anchor
		 * 
		 * @param <ADest> The dependency anchor type
		 * @param anchorType The dependency anchor type
		 * @param anchor The function to get the dependency anchor from the target anchor
		 * @return The metric definition precursor
		 */
		public <ADest extends Anchor> MetricDefiner<ASrc, ADest, T> anchor(AnchorType<ADest> anchorType, Function<ASrc, ADest> anchor) {
			return anchor(LambdaUtils.constantFn(anchorType, anchorType.getName(), true), anchor);
		}

		/**
		 * Use this if the type of the dependency anchor is constant, but the anchor is a function of the target anchor and metric
		 * 
		 * @param <ADest> The dependency anchor type
		 * @param anchorType The dependency anchor type
		 * @param anchor The function to get the dependency anchor from the target anchor/metric
		 * @return The metric definition precursor
		 */
		public <ADest extends Anchor> MetricDefiner<ASrc, ADest, T> anchor(AnchorType<ADest> anchorType,
			BiFunction<ASrc, Metric<T>, ADest> anchor) {
			return anchor(LambdaUtils.constantFn(anchorType, anchorType.getName(), true), anchor);
		}

		/**
		 * Use this if the type of the dependency anchor is dependent on the target anchor type and the anchor is a function of the target
		 * anchor
		 * 
		 * @param <ADest> The dependency anchor type
		 * @param anchorType The dependency anchor type
		 * @param anchor The function to get the dependency anchor from the target anchor
		 * @return The metric definition precursor
		 */
		public <ADest extends Anchor> MetricDefiner<ASrc, ADest, T> anchor(Function<AnchorType<ASrc>, AnchorType<ADest>> anchorType,
			Function<ASrc, ADest> anchor) {
			return anchor(anchorType,
				LambdaUtils.printableBiFn((a, m) -> anchor.apply(a), anchor.toString(), LambdaUtils.isIdentifiedByName(anchor)));
		}

		/**
		 * Use this if the type of the dependency anchor is a dependent on the target anchor and the anchor is a function of the target
		 * anchor and metric
		 * 
		 * @param <ADest> The dependency anchor type
		 * @param anchorType The dependency anchor type
		 * @param anchor The function to get the dependency anchor from the target anchor/metric
		 * @return The metric definition precursor
		 */
		public <ADest extends Anchor> MetricDefiner<ASrc, ADest, T> anchor(Function<AnchorType<ASrc>, AnchorType<ADest>> anchorType,
			BiFunction<ASrc, Metric<T>, ADest> anchor) {
			return new MetricDefiner<>(this, anchorType, anchor);
		}
	}

	/**
	 * Initial step for defining a dynamic dependency--defining the dependency anchor(s)
	 * 
	 * @param <ASrc> The type of the target anchor
	 * @param <T> The type of the target metric
	 */
	public static class DynamicAnchorDefiner<ASrc extends Anchor, T> extends AnchorDefiner<ASrc, T> {
		DynamicAnchorDefiner(String dependencyName) {
			super(dependencyName, false);
		}

		@Override
		public DynamicMetricDefiner<ASrc, ASrc, T> self() {
			return (DynamicMetricDefiner<ASrc, ASrc, T>) super.self();
		}

		@Override
		public DynamicMetricDefiner<ASrc, WorldAnchor, T> world() {
			return (DynamicMetricDefiner<ASrc, WorldAnchor, T>) super.world();
		}

		@Override
		public <ADest extends Anchor> DynamicMetricDefiner<ASrc, ADest, T> anchor(AnchorType<ADest> anchorType,
			Function<ASrc, ADest> anchor) {
			return (DynamicMetricDefiner<ASrc, ADest, T>) super.anchor(anchorType, anchor);
		}

		@Override
		public <ADest extends Anchor> DynamicMetricDefiner<ASrc, ADest, T> anchor(AnchorType<ADest> anchorType,
			BiFunction<ASrc, Metric<T>, ADest> anchor) {
			return (DynamicMetricDefiner<ASrc, ADest, T>) super.anchor(anchorType, anchor);
		}

		@Override
		public <ADest extends Anchor> DynamicMetricDefiner<ASrc, ADest, T> anchor(Function<AnchorType<ASrc>, AnchorType<ADest>> anchorType,
			Function<ASrc, ADest> anchor) {
			return (DynamicMetricDefiner<ASrc, ADest, T>) super.anchor(anchorType, anchor);
		}

		@Override
		public <ADest extends Anchor> DynamicMetricDefiner<ASrc, ADest, T> anchor(Function<AnchorType<ASrc>, AnchorType<ADest>> anchorType,
			BiFunction<ASrc, Metric<T>, ADest> anchor) {
			return new DynamicMetricDefiner<>(this, anchorType, anchor, null, false);
		}

		/**
		 * Use this if the type of the dependency anchor is constant and the anchor is singular and dependent on static dependency values
		 * 
		 * @param <ADest> The type of the dependency anchor
		 * @param anchorType The type of the dependency anchor
		 * @param anchor The function to produce the dependency anchor from static dependency values
		 * @return The metric definition precursor
		 */
		public <ADest extends Anchor> DynamicMetricDefiner<ASrc, ADest, T> dynamicAnchor(AnchorType<ADest> anchorType,
			Function<? super DependencyValueSet<ASrc, T>, ADest> anchor) {
			return dynamicAnchor(LambdaUtils.constantFn(anchorType, anchorType.getName(), true), anchor);
		}

		/**
		 * Use this if the type of the dependency anchor is dependent on the target anchor type and the anchor is singular and dependent on
		 * static dependency values
		 * 
		 * @param <ADest> The type of the dependency anchor
		 * @param anchorType The function to produce the type of the dependency anchor from the type of the target anchor
		 * @param anchor The function to produce the dependency anchor from static dependency values
		 * @return The metric definition precursor
		 */
		public <ADest extends Anchor> DynamicMetricDefiner<ASrc, ADest, T> dynamicAnchor(
			Function<AnchorType<ASrc>, AnchorType<ADest>> anchorType, Function<? super DependencyValueSet<ASrc, T>, ADest> anchor) {
			return _dynamicAnchors(anchorType,
				LambdaUtils.printableFn(ds -> singletonList(anchor.apply(ds)), () -> "listOf(" + anchor + ")"), false);
		}

		/**
		 * Use this if the type of the dependency anchor is constant and the anchor is multiple
		 * 
		 * @param <ADest> The type of the dependency anchor
		 * @param anchorType The type of the dependency anchor
		 * @param anchors The function to produce the dependency anchors from static dependency values
		 * @return The metric definition precursor
		 */
		public <ADest extends Anchor> DynamicMetricDefiner<ASrc, ADest, T> dynamicAnchors(AnchorType<ADest> anchorType,
			Function<? super DependencyValueSet<ASrc, T>, List<? extends ADest>> anchors) {
			return dynamicAnchors(LambdaUtils.constantFn(anchorType, anchorType.getName(), true), anchors);
		}

		/**
		 * Use this if the type of the dependency anchor is dependent on the target anchor type and the anchor is multiple
		 * 
		 * @param <ADest> The type of the dependency anchor
		 * @param anchorType The function to produce the type of the dependency anchor from the type of the target anchor
		 * @param anchors The function to produce the dependency anchors from static dependency values
		 * @return The metric definition precursor
		 */
		public <ADest extends Anchor> DynamicMetricDefiner<ASrc, ADest, T> dynamicAnchors(
			Function<AnchorType<ASrc>, AnchorType<ADest>> anchorType,
			Function<? super DependencyValueSet<ASrc, T>, List<? extends ADest>> anchors) {
			return _dynamicAnchors(anchorType, anchors, true);
		}

		private <ADest extends Anchor> DynamicMetricDefiner<ASrc, ADest, T> _dynamicAnchors(
			Function<AnchorType<ASrc>, AnchorType<ADest>> anchorType,
			Function<? super DependencyValueSet<ASrc, T>, List<? extends ADest>> anchors, boolean multi) {
			return new DynamicMetricDefiner<>(this, anchorType, null, anchors, multi);
		}
	}

	static <T> List<T> singletonList(T value) {
		if (value == null) {
			return Collections.emptyList();
		} else {
			return Arrays.asList(value);
		}
	}

	/**
	 * Second step in defining a static dependency--defining the dependency metric
	 * 
	 * @author abutler
	 *
	 * @param <ASrc> The type of the target anchor
	 * @param <ADest> The type of the dependency anchor
	 * @param <T> The type of the target metric
	 */
	public static class MetricDefiner<ASrc extends Anchor, ADest extends Anchor, T> {
		final AnchorDefiner<ASrc, T> thePrecursor;
		final Function<AnchorType<ASrc>, AnchorType<ADest>> theAnchorType;
		final BiFunction<ASrc, Metric<T>, ADest> theAnchor;

		MetricDefiner(AnchorDefiner<ASrc, T> precursor, Function<AnchorType<ASrc>, AnchorType<ADest>> anchorType,
			BiFunction<ASrc, Metric<T>, ADest> anchor) {
			thePrecursor = precursor;
			theAnchorType = anchorType;
			theAnchor = anchor;
		}

		/**
		 * Use this if the dependency metric is constant and not quantitative
		 * 
		 * @param <D> The type of the dependency metric
		 * @param metric The dependency metric
		 * @return The dependency options precursor
		 */
		public <D> Builder<ASrc, ADest, T, D> metric(Metric<D> metric) {
			return metric(metric.getType(), LambdaUtils.printableBiFn((a, m) -> metric, metric.toString(), true));
		}

		/**
		 * Use this if the dependency metric is neither constant nor quantitative
		 * 
		 * @param <D> The type of the dependency metric
		 * @param type The metric type of the dependency metric
		 * @param metric the function to produce the dependency metric from the target anchor/metric
		 * @return The dependency options precursor
		 */
		public <D> Builder<ASrc, ADest, T, D> metric(MetricType<D> type, BiFunction<ASrc, Metric<T>, Metric<D>> metric) {
			return new MetricDependencyType.Builder<>(thePrecursor.theDependencyName, thePrecursor.isAdaptive, //
				type, metric, theAnchorType, theAnchor);
		}
	}

	/**
	 * Second step in defining a dynamic dependency--defining the dependency metric(s)
	 * 
	 * @author abutler
	 *
	 * @param <ASrc> The type of the target anchor
	 * @param <ADest> The type of the dependency anchor
	 * @param <T> The type of the target metric
	 */
	public static class DynamicMetricDefiner<ASrc extends Anchor, ADest extends Anchor, T> extends MetricDefiner<ASrc, ADest, T> {
		final Function<? super DependencyValueSet<ASrc, T>, ? extends List<? extends ADest>> theDynamicAnchor;
		private final boolean isMulti;

		DynamicMetricDefiner(DynamicAnchorDefiner<ASrc, T> precursor, Function<AnchorType<ASrc>, AnchorType<ADest>> anchorType,
			BiFunction<ASrc, Metric<T>, ADest> anchor,
			Function<? super DependencyValueSet<ASrc, T>, ? extends List<? extends ADest>> dynamicAnchor, boolean multi) {
			super(precursor, anchorType, anchor);
			theDynamicAnchor = dynamicAnchor;
			isMulti = multi;
		}

		@Override
		public <X> MetricDependencyType.DynamicBuilder<ASrc, ADest, T, X> metric(Metric<X> metric) {
			return (MetricDependencyType.DynamicBuilder<ASrc, ADest, T, X>) super.metric(metric);
		}

		@Override
		public <X> MetricDependencyType.DynamicBuilder<ASrc, ADest, T, X> metric(MetricType<X> type,
			BiFunction<ASrc, Metric<T>, Metric<X>> metric) {
			return dynamicMetric(type, LambdaUtils.printableFn(ds -> metric.apply(ds.getAnchor(), ds.getMetric()), metric.toString(),
				LambdaUtils.isIdentifiedByName(metric)));
		}

		/**
		 * Use this if the dependency metric is singular, not quantitative, and dependent on static dependency values
		 * 
		 * @param <X> The type of the dependency metric
		 * @param type The metric type of the dependency metric
		 * @param metric The function to produce the dependency metric from static dependency values
		 * @return The dependency options precursor
		 */
		public <X> MetricDependencyType.DynamicBuilder<ASrc, ADest, T, X> dynamicMetric(MetricType<X> type,
			Function<? super DependencyValueSet<ASrc, T>, Metric<X>> metric) {
			return _dynamicMetrics(type, LambdaUtils.printableFn(ds -> singletonList(metric.apply(ds)), () -> "listOf(" + metric + ")"),
				false);
		}

		/**
		 * Use this if the dependency metric is multiple and not quantitative
		 * 
		 * @param <X> The type of the dependency metric
		 * @param type The metric type of the dependency metric
		 * @param metrics The function to produce the dependency metrics from static dependency values
		 * @return The dependency options precursor
		 */
		public <X> MetricDependencyType.DynamicBuilder<ASrc, ADest, T, X> dynamicMetrics(MetricType<X> type,
			Function<? super DependencyValueSet<ASrc, T>, ? extends List<? extends Metric<X>>> metrics) {
			return _dynamicMetrics(type, metrics, true);
		}

		private <X> MetricDependencyType.DynamicBuilder<ASrc, ADest, T, X> _dynamicMetrics(MetricType<X> type,
			Function<? super DependencyValueSet<ASrc, T>, ? extends List<? extends Metric<X>>> metrics, boolean multi) {
			return new MetricDependencyType.DynamicBuilder<>(thePrecursor.theDependencyName, //
				type, metrics, theAnchorType, theAnchor, theDynamicAnchor, isMulti || multi);
		}
	}

	/**
	 * Final step in defining a static dependency--additional options
	 * 
	 * @param <ASrc> The type of the target anchor
	 * @param <ADest> The type of the dependency anchor
	 * @param <T> The type of the target metric
	 * @param <X> The type of the dependency metric
	 */
	public static class Builder<ASrc extends Anchor, ADest extends Anchor, T, X> {
		private final String theName;
		private final boolean isAdaptive;
		private final MetricType<X> theMetricType;
		private final BiFunction<ASrc, Metric<T>, ? extends Metric<X>> theMetric;
		private final Function<AnchorType<ASrc>, AnchorType<ADest>> theAnchorType;
		private final BiFunction<ASrc, Metric<T>, ADest> theAnchor;

		private Function<? super DependencyTypeSet, DependencyType> isRequired;
		private Function<? super DependencyInstanceSet<ASrc, T>, DependencyType> isInstanceRequired;

		Builder(String name, boolean adaptive, //
			MetricType<X> metricType, BiFunction<ASrc, Metric<T>, ? extends Metric<X>> metric, //
			Function<AnchorType<ASrc>, AnchorType<ADest>> anchorType, BiFunction<ASrc, Metric<T>, ADest> anchor) {
			theName = name;
			isAdaptive = adaptive;
			theMetricType = metricType;
			theMetric = metric;
			theAnchorType = anchorType;
			theAnchor = anchor;
			isRequired = LambdaUtils.constantFn(DependencyType.Required, "Required", true);
			isInstanceRequired = LambdaUtils.printableFn(ds -> this.isRequired.apply(ds), () -> isRequired.toString());
		}

		/** @return The function to determine whether the dependency is required based on little dependency information */
		protected Function<? super DependencyTypeSet, DependencyType> isRequired() {
			return isRequired;
		}

		/** @return The function to determine whether the dependency is required based on more dependency information */
		protected Function<? super DependencyInstanceSet<Anchor, T>, DependencyType> isInstanceRequired() {
			return (Function<? super DependencyInstanceSet<Anchor, T>, DependencyType>) isInstanceRequired;
		}

		/** @return The name of the dependency */
		protected String getName() {
			return theName;
		}

		/** @return The metric type of the dependency */
		protected MetricType<X> getMetricType() {
			return theMetricType;
		}

		/** @return The function to produce the dependency metric from the target anchor/metric */
		protected BiFunction<Anchor, Metric<T>, ? extends Metric<X>> getMetric() {
			return (BiFunction<Anchor, Metric<T>, ? extends Metric<X>>) theMetric;
		}

		/** @return The function to produce the dependency anchor type from the target anchor type */
		protected Function<AnchorType<ASrc>, AnchorType<ADest>> getAnchorType() {
			return theAnchorType;
		}

		/** @return The function to produce the dependency anchor from the target anchor/metric */
		protected BiFunction<Anchor, Metric<T>, ? extends Anchor> getAnchor() {
			return (BiFunction<Anchor, Metric<T>, ? extends Anchor>) theAnchor;
		}

		/**
		 * Marks this dependency as optional, i.e. it will be available if supported, but the target value can be computed without it
		 * 
		 * @return This builder
		 */
		public Builder<ASrc, ADest, T, X> optional() {
			isRequired = LambdaUtils.printableFn(ds -> DependencyType.Optional, "Optional", true);
			return this;
		}

		/**
		 * Defines the needfulness of this dependency as a function of little dependency information
		 * 
		 * @param required The dependency type function
		 * @return This builder
		 */
		public Builder<ASrc, ADest, T, X> requiredIf(Function<? super DependencyTypeSet, DependencyType> required) {
			isRequired = required;
			return this;
		}

		/**
		 * Defines the needfulness of this dependency as a function of more dependency information
		 * 
		 * @param required The dependency type function
		 * @return This builder
		 */
		public Builder<ASrc, ADest, T, X> requiredIfInstance(Function<? super DependencyInstanceSet<ASrc, T>, DependencyType> required) {
			isInstanceRequired = required;
			return this;
		}

		/** @return The dependency */
		public MetricDependencyType<T, X> build() {
			return new MetricDependencyType<>(theName, theMetricType, getMetric(), theAnchorType, getAnchor(), //
				isRequired, isInstanceRequired(), //
				null, null, null, null, isAdaptive, false);
		}
	}

	/**
	 * Final step in defining a dynamic dependency--additional options
	 * 
	 * @param <ASrc> The type of the target anchor
	 * @param <ADest> The type of the dependency anchor
	 * @param <T> The type of the target metric
	 * @param <X> The type of the dependency metric
	 */
	public static class DynamicBuilder<ASrc extends Anchor, ADest extends Anchor, T, X> extends Builder<ASrc, ADest, T, X> {
		private final Function<? super DependencyValueSet<ASrc, T>, ? extends List<? extends Anchor>> theDynamicAnchor;
		private final Function<? super DependencyValueSet<ASrc, T>, ? extends List<? extends Metric<X>>> theDynamicMetric;
		private Function<? super DependencyValueSet<ASrc, T>, DependencyType> isDynamicallyRequired;
		private ConditionalChannelFilter<ASrc, T, X> theChannelFilter;
		private final boolean isMulti;

		DynamicBuilder(String name, MetricType<X> metricType,
			Function<? super DependencyValueSet<ASrc, T>, ? extends List<? extends Metric<X>>> metrics,
			Function<AnchorType<ASrc>, AnchorType<ADest>> anchorType, BiFunction<ASrc, Metric<T>, ADest> anchor,
			Function<? super DependencyValueSet<ASrc, T>, ? extends List<? extends Anchor>> dynamicAnchor, boolean multi) {
			super(name, false, metricType, null, anchorType, anchor);
			theDynamicAnchor = dynamicAnchor;
			theDynamicMetric = metrics;
			isDynamicallyRequired = null;
			theChannelFilter = null;
			isMulti = multi;
		}

		/** @return The function to produce the dependency anchors from static dependency values */
		protected Function<? super DependencyValueSet<Anchor, T>, ? extends List<? extends Anchor>> getDynamicAnchor() {
			return (Function<? super DependencyValueSet<Anchor, T>, ? extends List<? extends Anchor>>) theDynamicAnchor;
		}

		/** @return The function to produce the dependency metrics from static dependency values */
		protected Function<? super DependencyValueSet<Anchor, T>, ? extends List<? extends Metric<X>>> getDynamicMetric() {
			return (Function<? super DependencyValueSet<Anchor, T>, ? extends List<? extends Metric<X>>>) theDynamicMetric;
		}

		/** @return The function that determines the needfulness of this dependency from the static dependency values */
		protected Function<? super DependencyValueSet<Anchor, T>, DependencyType> isDynamicallyRequired() {
			return (Function<? super DependencyValueSet<Anchor, T>, DependencyType>) isDynamicallyRequired;
		}

		/** @return The channel filter for this dependency */
		protected ConditionalChannelFilter<Anchor, T, X> getChannelFilter() {
			return (ConditionalChannelFilter<Anchor, T, X>) theChannelFilter;
		}

		@Override
		public DynamicBuilder<ASrc, ADest, T, X> optional() {
			super.optional();
			return this;
		}

		@Override
		public DynamicBuilder<ASrc, ADest, T, X> requiredIf(Function<? super DependencyTypeSet, DependencyType> required) {
			super.requiredIf(required);
			return this;
		}

		@Override
		public DynamicBuilder<ASrc, ADest, T, X> requiredIfInstance(
			Function<? super DependencyInstanceSet<ASrc, T>, DependencyType> required) {
			super.requiredIfInstance(required);
			return this;
		}

		/**
		 * Specifies that the needfulness of this dependency depends on static metric values
		 * 
		 * @param required The function to determine the needfulness of this dependency from the static dependency values
		 * @return This builder
		 */
		public DynamicBuilder<ASrc, ADest, T, X> dynamicallyRequiredIf(
			Function<? super DependencyValueSet<ASrc, T>, DependencyType> required) {
			isDynamicallyRequired = required;
			return this;
		}

		/**
		 * Specifies that this dependency filters its channels based on static dependency values
		 * 
		 * @param filter The channel filter
		 * @return This builder
		 */
		public DynamicBuilder<ASrc, ADest, T, X> filterChannels(ConditionalChannelFilter<ASrc, T, X> filter) {
			theChannelFilter = filter;
			return this;
		}

		@Override
		public MetricDependencyType<T, X> build() {
			return new MetricDependencyType<>(getName(), getMetricType(), getMetric(), getAnchorType(), getAnchor(), //
				isRequired(), isInstanceRequired(), //
				getDynamicAnchor(), getDynamicMetric(), //
				isDynamicallyRequired(), getChannelFilter(), false, isMulti);
		}
	}
}
