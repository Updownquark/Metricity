package org.metricity.metric.util;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.metricity.anchor.Anchor;
import org.metricity.anchor.AnchorType;
import org.metricity.metric.Metric;
import org.metricity.metric.MetricTypeBuilder;
import org.metricity.metric.MetricTypeBuilder.SimpleMetricTypeBuilder;
import org.metricity.metric.MultiRelationMetricType;
import org.metricity.metric.RelationMetricType;
import org.metricity.metric.SimpleMetric;
import org.metricity.metric.SimpleMetricType;
import org.observe.Observable;
import org.observe.SimpleObservable;
import org.observe.util.TypeTokens;
import org.qommons.StringUtils;
import org.qommons.Transaction;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

/**
 * <p>
 * A metric model is a set of metrics whose implementation may be switched out using the value of the "model" parameter.
 * </p>
 * 
 * <p>
 * To use this class, create a direct, non-abstract extension of this class. Within this class, define a singleton field of the class's
 * type, whose instantiation should pass the name of the model type as the <code>modelName</code> parameter and false as the
 * <code>modelExposed</code>) and define a static getter method for this singleton field named <code>getInstance()</code>. This class is the
 * definition of a model type. This class may define static {@link MetricPlaceHolder}-type fields using the
 * {@link #addSimple(String, Class, Consumer)}, {@link #addQuantitative(String, Class, Consumer)},
 * {@link #addRelation(String, Class, String, Consumer)}, etc. methods. These metrics must be satisfied for every model of the type.
 * </p>
 * <p>
 * To create a model of the above-defined type, create an extension of that class, also with a singleton field and a
 * <code>getInstance()</code> accessor, whose <code>modelName</code> is the name of the model that the class defines and with
 * <code>modelExposed</code> true. This class may specify additional {@link MetricPlaceHolder metric} fields that must be satisfied only for
 * that model and any sub-models, which are subclasses of the model, defined similarly.
 * </p>
 * 
 * <p>
 * The "model" parameter is an enumerated parameter whose values correspond to the <code>modelName</code> of every model class with the same
 * immediate subclass.
 * </p>
 */
public abstract class MetricModel {
	/** The name of the "model" parameter */
	public static final String MODEL_PARAM = "model";
	/** The "Selected" model */
	public static final String SELECTED = "Selected";

	/**
	 * A metric placeholder that produces {@link Metric}s for a model
	 *
	 * @param <T> The type of the metric
	 */
	public static interface MetricPlaceHolder<T> {
		/** @return The model metric type represented by this placeholder */
		SimpleMetricType<T> get();

		/**
		 * @return The model metric represented by this placeholder, with the {@link MetricModel#SELECTED "Selected"}
		 *         {@link MetricModel#MODEL_PARAM model}
		 */
		default SimpleMetric<T> getMetric() {
			return get().build().with(MODEL_PARAM, SELECTED).build();
		}

		/**
		 * A relation metric placeholder that produces {@link RelationMetricType relation metric}s for a model
		 *
		 * @param <A> The type of the related anchor
		 */
		public static interface Relation<A extends Anchor> extends MetricPlaceHolder<A> {
			@Override
			RelationMetricType.Simple<A> get();
		}

		/**
		 * A multi-relation metric placeholder that produces {@link MultiRelationMetricType multi-relation metric}s for a model
		 *
		 * @param <A> The type of the related anchor
		 * @param <C> The type of the anchor collection
		 */
		public static interface MultiRelation<A extends Anchor, C extends Collection<A>> extends MetricPlaceHolder<C> {
			@Override
			MultiRelationMetricType.Simple<A, C> get();
		}
	}

	private final MetricModel theRoot;
	private final MetricModel theParent;
	private final String theModelName;
	private final boolean isModelExposed;
	private final NavigableMap<String, MetricModel> theChildren;
	private final Map<String, MetricPlaceHolderImpl<?, ?, ?>> theModelMetrics;
	private final Map<String, MetricPlaceHolderImpl<?, ?, ?>> theGlobalMetrics;
	private final List<SimpleMetricType<?>> theCurrentMetrics;
	private boolean isCurrentMetricsDirty;

	private final ReentrantReadWriteLock theLock;
	private final SimpleObservable<Object> theChanges;

	/**
	 * @param modelName The model type name or model name for the class
	 * @param modelExposed Whether this is a model type (false) or instance (true)
	 */
	protected MetricModel(String modelName, boolean modelExposed) {
		MetricModel inst;
		try {
			inst = (MetricModel) getClass().getDeclaredMethod("getInstance").invoke(null);
		} catch (Exception e) {
			throw new IllegalStateException("Could not get instance for model " + getClass().getName(), e);
		}
		if (inst != null) {
			throw new IllegalStateException("Only one instance per metric model type may be created");
		}

		theModelName = modelName;
		isModelExposed = modelExposed;
		theChildren = new TreeMap<>(StringUtils.DISTINCT_NUMBER_TOLERANT);
		theModelMetrics = new LinkedHashMap<>();
		theCurrentMetrics = new ArrayList<>();

		Class<? extends MetricModel> superClazz = (Class<? extends MetricModel>) getClass().getSuperclass();
		if ((superClazz.getModifiers() & Modifier.ABSTRACT) == 0) {
			// Concrete class, may have actual metrics
			MetricModel parent;
			try {
				parent = (MetricModel) superClazz.getDeclaredMethod("getInstance").invoke(null);
			} catch (Exception e) {
				throw new IllegalStateException("Could not get instance for model " + getClass().getName(), e);
			}
			theParent = parent;
			theRoot = parent.theRoot;
			theGlobalMetrics = parent.theGlobalMetrics;
			theLock = theRoot.theLock;
			theChanges = theRoot.theChanges;
			try (Transaction t = lock(true)) {
				MetricModel old = theParent.theChildren.put(theModelName, this);
				if (old != null) {
					throw new IllegalStateException("Multiple children of " + theParent.theModelName + " (" + theParent.getClass().getName()
						+ ") named " + theModelName + ": " + old.getClass().getName() + " and " + getClass().getName());
				}
				if (isModelExposed) {
					MetricModel p = theParent;
					while (p != null) {
						for (MetricPlaceHolderImpl<?, ?, ?> metric : p.theModelMetrics.values()) {
							metric.addModel(theModelName);
						}
						p.isCurrentMetricsDirty = true;
						p = p.theParent;
					}
				}
			}
		} else {
			// We're the root model
			theParent = null;
			theRoot = this;
			theGlobalMetrics = new LinkedHashMap<>();
			theLock = new ReentrantReadWriteLock();
			theChanges = new SimpleObservable<>();
		}
	}

	/** @return The name of this model or model type */
	public String getModelName() {
		return theModelName;
	}

	/**
	 * Puts a hold on this model's hierarchy
	 * 
	 * @param write Whether to allow addition of models or sub-models on this thread while the lock is held
	 * @return The transaction to {@link Transaction#close()} to release the lock
	 */
	public Transaction lock(boolean write) {
		Lock lock = write ? theLock.writeLock() : theLock.readLock();
		lock.lock();
		return lock::unlock;
	}

	private <T, C extends Collection<T>> C add(C collection, boolean withAncestors, boolean withDescendants, Predicate<MetricModel> filter,
		Function<MetricModel, T> value) {
		if (withAncestors && theParent != null) {
			theParent.add(collection, true, false, filter, value);
		}
		if (filter == null || filter.test(this)) {
			collection.add(value.apply(this));
		}
		if (withDescendants) {
			for (MetricModel child : theChildren.values()) {
				child.add(collection, false, true, filter, value);
			}
		}
		return collection;
	}

	private <T, C extends Collection<T>> C addAll(C collection, boolean withAncestors, boolean withDescendants,
		Predicate<MetricModel> filter, Function<MetricModel, ? extends Collection<? extends T>> values) {
		if (withAncestors && theParent != null) {
			theParent.addAll(collection, true, false, filter, values);
		}
		if (filter == null || filter.test(this)) {
			collection.addAll(values.apply(this));
		}
		if (withDescendants) {
			for (MetricModel child : theChildren.values()) {
				child.addAll(collection, false, true, filter, values);
			}
		}
		return collection;
	}

	/** @return The exposed models defined at or under this model */
	public NavigableSet<String> getAvailableModels() {
		try (Transaction t = lock(false)) {
			return Collections.unmodifiableNavigableSet(
				add(new TreeSet<>(StringUtils.DISTINCT_NUMBER_TOLERANT), true, true, m -> m.isModelExposed, m -> m.theModelName));
		}
	}

	/** @return The metric types defined by this model and its sub-models */
	public List<SimpleMetricType<?>> getMetrics() {
		try (Transaction t = lock(false)) {
			return Collections.unmodifiableList(addAll(new ArrayList<>(), true, true, null, m -> m.getCurrentMetrics()));
		}
	}

	private List<SimpleMetricType<?>> getCurrentMetrics() {
		if (isCurrentMetricsDirty) {
			theCurrentMetrics.clear();
			for (MetricPlaceHolderImpl<?, ?, ?> ph : theModelMetrics.values()) {
				theCurrentMetrics.add(ph.get());
			}
			isCurrentMetricsDirty = false;
		}
		return theCurrentMetrics;
	}

	/**
	 * @param metric The metric to get the model for
	 * @return The list of models that define a metric with the given name
	 */
	public List<String> getModels(String metric) {
		try (Transaction t = lock(false)) {
			return Collections
				.unmodifiableList(add(new ArrayList<>(), true, true, m -> theModelMetrics.containsKey(metric), m -> m.theModelName));
		}
	}

	/**
	 * @param model The sub-model to get the metrics for
	 * @return All metrics defined by the given sub-model
	 */
	public List<SimpleMetricType<?>> getMetrics(String model) {
		List<SimpleMetricType<?>> metrics = _getMetrics(model);
		if (metrics == null) {
			throw new IllegalArgumentException("Unrecognized model: " + model);
		}
		return metrics;
	}

	private List<SimpleMetricType<?>> _getMetrics(String model) {
		try (Transaction t = lock(false)) {
			if (model.equals(theModelName)) {
				return Collections.unmodifiableList(addAll(new ArrayList<>(), true, false, null, m -> m.getCurrentMetrics()));
			}
			{
				MetricModel child = theChildren.get(model);
				if (child != null) {
					return child._getMetrics(model);
				}
			}
			List<SimpleMetricType<?>> metrics = null;
			for (MetricModel child : theChildren.values()) {
				metrics = child._getMetrics(model);
				if (metrics != null) {
					break;
				}
			}
			return metrics;
		}
	}

	/** @return An observable that fires when a new sub-model is defined */
	public Observable<?> changes() {
		return theChanges.readOnly();
	}

	/** Causes the {@link #changes()} observable to fire, should be called at the end of all model definitions */
	protected void changed() {
		theChanges.onNext(null);
	}

	private <T, //
		M extends SimpleMetricType<T>, //
		B extends MetricTypeBuilder.SimpleMetricTypeBuilder<T>, //
		PH extends MetricPlaceHolderImpl<T, M, B>> //
	/*   */ PH add(PH metric) {
		try (Transaction t = lock(true)) {
			MetricPlaceHolderImpl<?, ?, ?> current = theGlobalMetrics.get(metric.name);
			if (current != null) {
				if (!current.equals(metric)) {
					throw new IllegalStateException("A metric named " + metric.name + " has been declared by " + getModels(metric.name)
						+ " as " + metric + ", but by " + theModelName + " as " + metric);
				}
				metric = (PH) current;
			} else {
				theGlobalMetrics.put(metric.name, metric);
				metric.init();
			}
			if (isModelExposed) {
				metric.addModel(theModelName);
			}
			theModelMetrics.put(metric.name, metric);
			isCurrentMetricsDirty = true;
			return metric;
		}
	}

	@Override
	public String toString() {
		return theModelName;
	}

	/**
	 * Defines a simple model metric
	 * 
	 * @param <T> The type of the metric
	 * @param name The name of the metric
	 * @param type The type of the metric
	 * @param create The builder for the metric
	 * @return The placeholder to represent the metric
	 */
	protected <T> MetricPlaceHolder<T> addSimple(String name, TypeToken<T> type,
		Consumer<MetricTypeBuilder.SimpleMetricTypeBuilder<T>> create) {
		return add(new MetricPlaceHolderImpl.SimpleImpl<>(name, type, create));
	}

	/**
	 * Defines a simple model metric
	 * 
	 * @param <T> The type of the metric
	 * @param name The name of the metric
	 * @param type The type of the metric
	 * @param create The builder for the metric
	 * @return The placeholder to represent the metric
	 */
	protected <T> MetricPlaceHolder<T> addSimple(String name, Class<T> type,
		Consumer<MetricTypeBuilder.SimpleMetricTypeBuilder<T>> create) {
		return addSimple(name, TypeTokens.get().of(type), create);
	}

	/**
	 * Defines a relation model metric
	 * 
	 * @param <A> The type of the related anchor
	 * @param name The name of the metric
	 * @param type The type of the related anchor
	 * @param anchorType The anchor type of the related anchor
	 * @param create The builder for the metric
	 * @return The placeholder to represent the metric
	 */
	protected <A extends Anchor> MetricPlaceHolder.Relation<A> addRelation(String name, TypeToken<A> type, AnchorType<A> anchorType,
		Consumer<MetricTypeBuilder.SimpleMetricTypeBuilder<A>> create) {
		return add(new MetricPlaceHolderImpl.RelationImpl<>(name, type, anchorType, create));
	}

	/**
	 * Defines a relation model metric
	 * 
	 * @param <A> The type of the related anchor
	 * @param name The name of the metric
	 * @param type The type of the related anchor
	 * @param anchorType The anchor type of the related anchor
	 * @param create The builder for the metric
	 * @return The placeholder to represent the metric
	 */
	protected <A extends Anchor> MetricPlaceHolder.Relation<A> addRelation(String name, Class<A> type, AnchorType<A> anchorType,
		Consumer<MetricTypeBuilder.SimpleMetricTypeBuilder<A>> create) {
		return addRelation(name, TypeTokens.get().of(type), anchorType, create);
	}

	/**
	 * Defines a multi-relation model metric
	 * 
	 * @param <A> The type of the related anchor
	 * @param <C> The type of the anchor collection
	 * @param name The name of the metric
	 * @param type The type of the related anchor
	 * @param anchorType The anchor type of the related anchor
	 * @param create The builder for the metric
	 * @return The placeholder to represent the metric
	 */
	protected <A extends Anchor, C extends Collection<A>> MetricPlaceHolder.MultiRelation<A, C> addMultiRelation(String name,
		TypeToken<C> type, AnchorType<A> anchorType, Consumer<MetricTypeBuilder.SimpleMetricTypeBuilder<C>> create) {
		return add(new MetricPlaceHolderImpl.MultiRelation<>(name, type, anchorType, create));
	}

	/**
	 * Defines a multi-relation model metric
	 * 
	 * @param <A> The type of the related anchor
	 * @param name The name of the metric
	 * @param type The type of the related anchor
	 * @param anchorType The anchor type of the related anchor
	 * @param create The builder for the metric
	 * @return The placeholder to represent the metric
	 */
	protected <A extends Anchor> MetricPlaceHolder.MultiRelation<A, List<A>> addMultiRelation(String name, Class<A> type,
		AnchorType<A> anchorType, Consumer<MetricTypeBuilder.SimpleMetricTypeBuilder<List<A>>> create) {
		TypeToken<List<A>> listType = new TypeToken<List<A>>() {}.where(new TypeParameter<A>() {}, TypeTokens.get().of(type));
		return add(new MetricPlaceHolderImpl.MultiRelation<>(name, listType, anchorType, create));
	}

	private static abstract class MetricPlaceHolderImpl<T, M extends SimpleMetricType<T>, B extends MetricTypeBuilder.SimpleMetricTypeBuilder<T>>
		implements MetricPlaceHolder<T> {
		final String name;
		private M currentMetric;

		MetricPlaceHolderImpl(String name) {
			this.name = name;
		}

		final void init() {
			B b = createBuilder();
			b.withParameter(MODEL_PARAM, TypeTokens.get().STRING, p -> p.withAllowedValues(SELECTED).withDefault(SELECTED));
			currentMetric = build(b);
		}

		final void addModel(String modelName) {
			List<String> currentModels = (List<String>) currentMetric.getParameters().get(MODEL_PARAM).getAllowedValues();
			List<String> models = new ArrayList<>(currentModels.size() + 1);
			boolean added = false;
			models.add(SELECTED);
			for (int i = 1; i < currentModels.size(); i++) {
				String model = currentModels.get(i);
				if (!added && StringUtils.compareNumberTolerant(modelName, model, true, true) < 0) {
					models.add(modelName);
					added = true;
				}
				models.add(model);
			}
			if (!added) {
				models.add(modelName);
			}

			B b = createBuilder();
			b.withParameter(MODEL_PARAM, TypeTokens.get().STRING, p -> p.withAllowedValues(models).withDefault(SELECTED));
			currentMetric = build(b);
		}

		abstract B createBuilder();

		abstract M build(B builder);

		@Override
		public final M get() {
			return currentMetric;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			} else if (getClass() != o.getClass()) {
				return false;
			}
			MetricPlaceHolderImpl<?, ?, ?> other = (MetricPlaceHolderImpl<?, ?, ?>) o;
			return name.equals(other.name);
		}

		@Override
		public String toString() {
			return name;
		}

		private static class SimpleImpl<T>
			extends MetricPlaceHolderImpl<T, SimpleMetricType<T>, MetricTypeBuilder.SimpleMetricTypeBuilder<T>> {
			private final TypeToken<T> type;
			private final Consumer<SimpleMetricTypeBuilder<T>> creator;

			SimpleImpl(String name, TypeToken<T> type, Consumer<MetricTypeBuilder.SimpleMetricTypeBuilder<T>> create) {
				super(name);
				this.type = type;
				this.creator = create;
			}

			@Override
			MetricTypeBuilder.SimpleMetricTypeBuilder<T> createBuilder() {
				MetricTypeBuilder.SimpleMetricTypeBuilder<T> builder = MetricTypeBuilder.build(name, type);
				if (creator != null) {
					creator.accept(builder);
				}
				return builder;
			}

			@Override
			SimpleMetricType<T> build(SimpleMetricTypeBuilder<T> builder) {
				return builder.build();
			}

			@Override
			public boolean equals(Object o) {
				return super.equals(o) && type.equals(((SimpleImpl<?>) o).type);
			}

			@Override
			public String toString() {
				return super.toString() + " (" + type + ")";
			}
		}

		private static class RelationImpl<A extends Anchor>
			extends MetricPlaceHolderImpl<A, RelationMetricType.Simple<A>, MetricTypeBuilder.SimpleMetricTypeBuilder<A>>
			implements MetricPlaceHolder.Relation<A> {
			private final TypeToken<A> type;
			private final AnchorType<A> anchorType;
			private final Consumer<MetricTypeBuilder.SimpleMetricTypeBuilder<A>> creator;

			RelationImpl(String name, TypeToken<A> type, AnchorType<A> anchorType, Consumer<SimpleMetricTypeBuilder<A>> create) {
				super(name);
				this.type = type;
				this.anchorType = anchorType;
				this.creator = create;
			}

			@Override
			MetricTypeBuilder.SimpleMetricTypeBuilder<A> createBuilder() {
				MetricTypeBuilder.SimpleMetricTypeBuilder<A> builder = MetricTypeBuilder.build(name, type);
				if (creator != null) {
					creator.accept(builder);
				}
				return builder;
			}

			@Override
			RelationMetricType.Simple<A> build(SimpleMetricTypeBuilder<A> builder) {
				return builder.buildRelation(anchorType);
			}

			@Override
			public boolean equals(Object o) {
				return super.equals(o) && anchorType.equals(((RelationImpl<?>) o).anchorType);
			}

			@Override
			public String toString() {
				return super.toString() + " (" + anchorType + ")";
			}
		}

		private static class MultiRelation<A extends Anchor, C extends Collection<A>>
			extends MetricPlaceHolderImpl<C, MultiRelationMetricType.Simple<A, C>, MetricTypeBuilder.SimpleMetricTypeBuilder<C>>
			implements MetricPlaceHolder.MultiRelation<A, C> {
			private final TypeToken<C> type;
			private final AnchorType<A> anchorType;
			private final Consumer<MetricTypeBuilder.SimpleMetricTypeBuilder<C>> creator;

			MultiRelation(String name, TypeToken<C> type, AnchorType<A> anchorType,
				Consumer<MetricTypeBuilder.SimpleMetricTypeBuilder<C>> create) {
				super(name);
				this.type = type;
				this.anchorType = anchorType;
				this.creator = create;
			}

			@Override
			MetricTypeBuilder.SimpleMetricTypeBuilder<C> createBuilder() {
				MetricTypeBuilder.SimpleMetricTypeBuilder<C> builder = MetricTypeBuilder.build(name, type);
				if (creator != null) {
					creator.accept(builder);
				}
				return builder;
			}

			@Override
			MultiRelationMetricType.Simple<A, C> build(SimpleMetricTypeBuilder<C> builder) {
				return builder.buildMultiRelation(anchorType);
			}

			@Override
			public boolean equals(Object o) {
				return super.equals(o) && anchorType.equals(((RelationImpl<?>) o).anchorType);
			}

			@Override
			public String toString() {
				return super.toString() + " <" + anchorType + ">";
			}
		}
	}
}
