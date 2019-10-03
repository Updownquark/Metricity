package org.metricity.metric;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.metricity.anchor.Anchor;
import org.metricity.anchor.AnchorType;
import org.metricity.metric.MetricParameterSet.MetricParameterMap;
import org.metricity.metric.RelatedMetricType.RelationMetricParameter;
import org.metricity.metric.util.MetricUtils;
import org.observe.util.TypeTokens;
import org.qommons.BreakpointHere;
import org.qommons.StringUtils;
import org.qommons.collect.QuickSet;
import org.qommons.collect.QuickSet.CustomOrderedQuickMap;
import org.qommons.collect.QuickSet.QuickMap;

import com.google.common.reflect.TypeToken;

/** Builds {@link MetricType}s */
public class MetricTypeBuilder {
	/**
	 * Builds a precursor that can be used to create metric types with less code
	 * 
	 * @param name The name of the parameter set
	 * @return The builder for the parameter set
	 */
	public static ParameterSetBuilder buildParamSet(String name) {
		return new ParameterSetBuilder(name);
	}

	/**
	 * Builds a {@link SimpleMetricType}
	 * 
	 * @param <T> The type of the metric
	 * @param name The name for the metric
	 * @param type The type for the metric
	 * @return The builder for the metric type
	 */
	public static <T> SimpleMetricTypeBuilder<T> build(String name, TypeToken<T> type) {
		return new SimpleMetricTypeBuilder<>(name, type);
	}

	/**
	 * Builds a {@link SimpleMetricType}
	 * 
	 * @param <T> The type of the metric
	 * @param name The name for the metric
	 * @param type The type for the metric
	 * @return The builder for the metric type
	 */
	public static <T> SimpleMetricTypeBuilder<T> build(String name, Class<T> type) {
		return build(name, TypeTokens.get().of(type));
	}

	/**
	 * Builds a {@link RelatedMetricType}
	 * 
	 * @param <T> The type of the metric
	 * @param relation The relation metric to supply the anchor related to the target
	 * @param relative The relative metric to supply the value on the related anchor
	 * @return The metric type
	 */
	public static <T> RelatedMetricType<T> buildRelated(RelationMetricType<? extends Anchor> relation, MetricType<T> relative) {
		if (relative instanceof RelationMetricType) {
			return (RelatedMetricType<T>) new RelationRelatedMetricTypeImpl<Anchor>(relation, (RelationMetricType<Anchor>) relative);
		} else if (relative instanceof MultiRelationMetricType) {
			return (RelatedMetricType<T>) new MultiRelationRelatedMetricTypeImpl<>(relation, (MultiRelationMetricType<Anchor, ?>) relative);
		} else {
			return new RelatedMetricTypeImpl<>(relation, relative);
		}
	}

	/**
	 * Builds a {@link AggregateMetricType}
	 * 
	 * @param <T> The type of the relative metric
	 * @param <X> The type of the aggregate metric
	 * @param relation The relation metric to supply the anchor or anchors related to the target
	 * @param relative The relative metric to supply values to aggregate for each related anchor
	 * @param aggregation The aggregation to use on the relative metric values
	 * @return The metric type
	 */
	public static <T, X> AggregateMetricType<T, X> buildAggregate(MultiRelationMetricType<?, ?> relation, MetricType<T> relative,
		MetricAggregation<? super T, X> aggregation) {
		if ((relative instanceof RelationMetricType || relative instanceof MultiRelationMetricType)
			&& RelationMetricType.isRelation(aggregation.getType())) {
			AnchorType<?> targetAnchorType = relative instanceof RelationMetricType
				? ((RelationMetricType<?>) relative).getTargetAnchorType()
				: ((MultiRelationMetricType<?, ?>) relative).getTargetAnchorType();
			return (AggregateMetricType<T, X>) new RelationAggregateMetricTypeImpl<>(relation, relative,
				(AnchorType<Anchor>) targetAnchorType, (MetricAggregation<? super T, Anchor>) aggregation);
		} else if ((relative instanceof RelationMetricType || relative instanceof MultiRelationMetricType)
			&& MultiRelationMetricType.isMultiRelation(aggregation.getType())) {
			AnchorType<?> targetAnchorType = relative instanceof RelationMetricType
				? ((RelationMetricType<?>) relative).getTargetAnchorType()
				: ((MultiRelationMetricType<?, ?>) relative).getTargetAnchorType();
			return (AggregateMetricType<T, X>) new MultiRelationAggregateMetricTypeImpl<>(relation, relative,
				(AnchorType<Anchor>) targetAnchorType, (MetricAggregation<? super T, Collection<Anchor>>) aggregation);
		} else {
			return new AggregateMetricTypeImpl<>(relation, relative, aggregation);
		}
	}

	interface MetricInstanceCache {
		<X extends MetricParameterMap> X get(QuickMap<String, MetricParameterValue<?>> parameters,
			Function<QuickMap<String, MetricParameterValue<?>>, X> builder);
	}

	interface CachingMetricType {
		MetricInstanceCache getCache();
	}

	/** Builds a {@link MetricParameterSet} */
	public static class ParameterSetBuilder {
		private final String theName;
		private final Map<String, MetricParameter<?>> theParameters;
		private final List<MetricParameterSet> theParameterSets;

		ParameterSetBuilder(String name) {
			theName = name;
			theParameters = new LinkedHashMap<>();
			theParameterSets = new ArrayList<>(1);
		}

		/** @return The name for the {@link MetricParameterSet} */
		public String getName() {
			return theName;
		}

		/**
		 * @param QuickSet The parameter set whose parameters to include in the new parameter set
		 * @return This builder
		 */
		public ParameterSetBuilder withParameterSet(MetricParameterSet QuickSet) {
			theParameterSets.add(QuickSet);
			return this;
		}

		/**
		 * @param <P> The type of the new parameter
		 * @param name The name for the new parameter
		 * @param type The type for the new parameter
		 * @return This builder
		 */
		public <P> ParameterSetBuilder withParameter(String name, TypeToken<P> type) {
			return withParameter(name, type, options -> {});
		}

		/**
		 * @param <P> The type of the new parameter
		 * @param name The name for the new parameter
		 * @param type The type for the new parameter
		 * @param options The options for the new parameter
		 * @return This builder
		 */
		public <P> ParameterSetBuilder withParameter(String name, TypeToken<P> type, Consumer<MetricParameterTypeBuilder<P>> options) {
			MetricParameterTypeBuilder<P> builder = new MetricParameterTypeBuilder<>(name, type);
			options.accept(builder);
			for (MetricParameterSet ps : theParameterSets) {
				if (ps.getParameters().containsKey(name)) {
					throw new IllegalArgumentException("Parameter " + name + " already defined by parameter set " + ps.getName());
				}
			}
			if (theParameters.put(name, builder.build()) != null) {
				throw new IllegalArgumentException("Parameter " + name + " already defined");
			}
			return this;
		}

		/**
		 * @param <P> The type of the new parameter
		 * @param name The name for the new parameter
		 * @param type The type for the new parameter
		 * @return This builder
		 */
		public <P> ParameterSetBuilder withParameter(String name, Class<P> type) {
			return withParameter(name, TypeTokens.get().of(type));
		}

		/** @return The parameter map for the built object */
		protected CustomOrderedQuickMap<String, MetricParameter<?>> buildParameters() {
			return MetricTypeBuilder.buildParameters(theParameters, theParameterSets);
		}

		/** @return The new {@link MetricParameterSet} */
		public MetricParameterSet build() {
			return new ParameterSetImpl(getName(), buildParameters());
		}
	}

	/**
	 * Builds a {@link SimpleMetricType}
	 *
	 * @param <T> The type of the metric
	 */
	public static class SimpleMetricTypeBuilder<T> extends ParameterSetBuilder {
		private final TypeToken<T> theType;
		private String theGroupName;
		private String theDisplayName;
		private boolean isInternalOnly;
		private boolean isUI;

		SimpleMetricTypeBuilder(String name, TypeToken<T> type) {
			super(name);
			theType = type;
			isUI = true;
		}

		/** @return The type of the metric */
		public TypeToken<T> getType() {
			return theType;
		}

		@Override
		public SimpleMetricTypeBuilder<T> withParameterSet(MetricParameterSet QuickSet) {
			super.withParameterSet(QuickSet);
			return this;
		}

		@Override
		public <P> SimpleMetricTypeBuilder<T> withParameter(String name, TypeToken<P> type) {
			super.withParameter(name, type);
			return this;
		}

		@Override
		public <P> SimpleMetricTypeBuilder<T> withParameter(String name, TypeToken<P> type,
			Consumer<MetricParameterTypeBuilder<P>> options) {
			super.withParameter(name, type, options);
			return this;
		}

		@Override
		public <P> SimpleMetricTypeBuilder<T> withParameter(String name, Class<P> type) {
			super.withParameter(name, type);
			return this;
		}

		/**
		 * Specifies that the new metric type will be {@link MetricType#isInternalOnly() internal}
		 * 
		 * @return This builder
		 */
		public SimpleMetricTypeBuilder<T> internalOnly() {
			isInternalOnly = true;
			isUI = false;
			return this;
		}

		/** @return Whether the new metric type will be built {@link MetricType#isInternalOnly() internal-only} */
		public boolean isInternalOnly() {
			return isInternalOnly;
		}

		/**
		 * Specifies that the new metric type will be not be a {@link MetricType#isUI() UI} metric
		 * 
		 * @return This builder
		 */
		public SimpleMetricTypeBuilder<T> noUI() {
			isUI = false;
			return this;
		}

		/** @return Whether the metric to be built will be a {@link MetricType#isUI() UI} metric */
		public boolean isUI() {
			return isUI;
		}

		/**
		 * @param groupName The group name for the metric
		 * @return This builder
		 */
		public SimpleMetricTypeBuilder<T> withGroupName(String groupName) {
			theGroupName = groupName;
			return this;
		}

		/** @return The group name for the metric */
		public String getGroupName() {
			return theGroupName != null ? theGroupName : deriveGroupName();
		}

		/** @return The group name for the metric if none is specified */
		public String deriveGroupName() {
			Class<?> type = TypeTokens.getRawType(getType());
			if (Enum.class.isAssignableFrom(type)) {
				return type.getSimpleName();
			} else {
				return null;
			}
		}

		/**
		 * @param displayName The display name for the metric
		 * @return This builder
		 */
		public SimpleMetricTypeBuilder<T> withDisplayName(String displayName) {
			theDisplayName = displayName;
			return this;
		}

		/** @return The display name for the metric */
		public String getDisplayName() {
			return theDisplayName != null ? theDisplayName : getName();
		}

		/** @return The new {@link SimpleMetricType} */
		@Override
		public SimpleMetricType<T> build() {
			return new MetricTypeImpl<>(getName(), theType, getGroupName(), getDisplayName(), isInternalOnly, isUI, buildParameters());
		}

		/**
		 * @param <A> The type of the relation anchor
		 * @param targetAnchorType The anchor type of the relation
		 * @return The new relation metric type
		 */
		public <A extends Anchor> RelationMetricType.Simple<A> buildRelation(AnchorType<A> targetAnchorType) {
			return new RelationSimpleMetricTypeImpl<>(getName(), (TypeToken<A>) theType, getGroupName(), getDisplayName(), isInternalOnly,
				isUI, buildParameters(), targetAnchorType);
		}

		/**
		 * @param <A> The type of the relation anchor
		 * @param <C> The type of the relation anchor collection
		 * @param targetAnchorType The anchor type of the relation
		 * @return The new multi-relation metric type
		 */
		public <A extends Anchor, C extends Collection<A>> MultiRelationMetricType.Simple<A, C> buildMultiRelation(
			AnchorType<A> targetAnchorType) {
			return new MultiRelationSimpleMetricTypeImpl<>(getName(), (TypeToken<C>) theType, getGroupName(), getDisplayName(),
				isInternalOnly, isUI, buildParameters(), targetAnchorType);
		}
	}

	/**
	 * Builds a {@link MetricParameter}
	 *
	 * @param <P> The type of the parameter
	 */
	public static class MetricParameterTypeBuilder<P> {
		private final String theParamName;
		private final TypeToken<P> theType;
		private final Map<String, Predicate<? super P>> theFilters;
		private boolean isNullable;
		private List<P> theAllowedValues;
		private Optional<P> theDefault;

		MetricParameterTypeBuilder(String paramName, TypeToken<P> paramType) {
			theParamName = paramName;
			theType = paramType;
			theFilters = new LinkedHashMap<>();
			theAllowedValues = new LinkedList<>();
			theDefault = Optional.empty();
		}

		/** @return The name of the parameter */
		public String getName() {
			return theParamName;
		}

		/** @return The type of the parameter */
		public TypeToken<P> getType() {
			return theType;
		}

		/** @return The set of named filters for the parameter */
		protected Map<String, Predicate<? super P>> getFilters() {
			return theFilters;
		}

		/** @return The default value for the parameter */
		protected Optional<P> getDefault() {
			return theDefault;
		}

		/**
		 * @param nullable Whether this parameter should accept a null value
		 * @return This builder
		 */
		public MetricParameterTypeBuilder<P> nullable(boolean nullable) {
			isNullable = nullable;
			return this;
		}

		/**
		 * @param filterMessage The message for when the filter is violated
		 * @param filter The filter for acceptable parameter values
		 * @return This builder
		 */
		public MetricParameterTypeBuilder<P> withFilter(String filterMessage, Predicate<? super P> filter) {
			theFilters.put(filterMessage, filter);
			return this;
		}

		/**
		 * @param values The exclusive list of values allowed for the parameter
		 * @return This builder
		 */
		public MetricParameterTypeBuilder<P> withAllowedValues(P... values) {
			return withAllowedValues(Arrays.asList(values));
		}

		/**
		 * @param values The exclusive list of values allowed for the parameter
		 * @return This builder
		 */
		public MetricParameterTypeBuilder<P> withAllowedValues(List<? extends P> values) {
			theAllowedValues.addAll(values);
			return this;
		}

		/**
		 * @param defaultValue The default value for the parameter (may not be null)
		 * @return This builder
		 */
		public MetricParameterTypeBuilder<P> withDefault(P defaultValue) {
			theDefault = Optional.of(defaultValue);
			return this;
		}

		MetricParameter<P> build() {
			return new SimpleParameter<>(theParamName, theType, isNullable, theFilters, theAllowedValues, theDefault);
		}
	}

	private static <P> CustomOrderedQuickMap<String, P> buildParameters(Map<String, ? extends P> parameters,
		List<MetricParameterSet> parameterSets) {
		if (parameters.isEmpty()) {
			if (parameterSets.isEmpty()) {
				return new CustomOrderedQuickMap<>(QuickMap.empty(), Collections.emptySet());
			} else if (parameterSets.size() == 1 && parameterSets.get(0).getParameters() instanceof CustomOrderedQuickMap) {
				return (CustomOrderedQuickMap<String, P>) parameterSets.get(0).getParameters();
			}
		}
		if (!parameterSets.isEmpty()) {
			Map<String, P> paramsCopy = new LinkedHashMap<>(parameters);
			for (MetricParameterSet paramSet : parameterSets) {
				for (Map.Entry<String, ? extends MetricParameter<?>> param : paramSet.getParameters().entrySet()) {
					paramsCopy.put(param.getKey(), (P) param.getValue());
				}
			}
			parameters = paramsCopy;
		}
		QuickSet<String> paramKeys = new QuickSet<>(StringUtils.DISTINCT_NUMBER_TOLERANT, parameters.keySet());
		QuickMap<String, P> paramValues = paramKeys.createMap();
		for (Map.Entry<String, ? extends P> param : parameters.entrySet()) {
			paramValues.put(param.getKey(), param.getValue());
		}
		return new CustomOrderedQuickMap<>(paramValues.unmodifiable(),
			new QuickSet.CustomOrderedQuickSet<>(paramKeys, parameters.keySet()));
	}

	private static final int MAX_COMBOS_FOR_EXHAUSTIVE_CACHING = 128;

	private static MetricInstanceCache createCache(CustomOrderedQuickMap<String, ? extends MetricParameter<?>> params) {
		if (params.isEmpty()) {
			return new SingletonMetricCache();
		} else {
			int combos = 1;
			for (int i = 0; i < params.getMap().keySet().size(); i++) {
				List<?> paramValues = MetricUtils.getParamEnumValues(params.getMap().get(i));
				if (paramValues == null) {
					combos = 0;
					break;
				}
				int paramCombos = paramValues.size();
				combos *= paramCombos;
				if (combos > MAX_COMBOS_FOR_EXHAUSTIVE_CACHING) {
					break;
				}
			}
			if (combos > 0 && combos < MAX_COMBOS_FOR_EXHAUSTIVE_CACHING) {
				return new ExhaustiveMetricCache(combos);
			} else {
				return null;
			}
		}
	}

	private static class ParameterSetImpl implements MetricParameterSet, CachingMetricType {
		private final String theName;
		private final CustomOrderedQuickMap<String, MetricParameter<?>> theParameters;
		private final MetricInstanceCache theCache;

		ParameterSetImpl(String name, CustomOrderedQuickMap<String, MetricParameter<?>> parameters) {
			theName = name;
			theParameters = parameters;
			theCache = createCache(theParameters);
		}

		@Override
		public String getName() {
			return theName;
		}

		@Override
		public Map<String, ? extends MetricParameter<?>> getParameters() {
			return theParameters;
		}

		@Override
		public MetricInstanceCache getCache() {
			return theCache;
		}

		@Override
		public Builder build() {
			return build(null);
		}

		@Override
		public Builder build(MetricParameterMap initParameters) {
			return MetricBuilder.buildParams(this, initParameters);
		}

		@Override
		public int hashCode() {
			return theParameters.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			} else if (!(obj instanceof MetricParameterSet)) {
				return false;
			}
			MetricParameterSet other = (MetricParameterSet) obj;
			return theName.equals(other.getName()) && theParameters.equals(other.getParameters());
		}

		@Override
		public String toString() {
			StringBuilder str = new StringBuilder(theName);
			if (!theParameters.isEmpty()) {
				str.append(theParameters.values());
			}
			return str.toString();
		}
	}

	private static class MetricTypeImpl<T> implements SimpleMetricType<T>, CachingMetricType {
		private final String theName;
		private final TypeToken<T> theType;
		private final String theGroupName;
		private final String theDisplayName;
		private final boolean isInternalOnly;
		private final boolean isUI;
		private final CustomOrderedQuickMap<String, MetricParameter<?>> theParameters;
		private final MetricInstanceCache theCache;

		protected int hashCode;

		MetricTypeImpl(String name, TypeToken<T> type, String groupName, String displayName, boolean internalOnly, boolean ui,
			CustomOrderedQuickMap<String, MetricParameter<?>> parameters) {
			if (name == null) {
				throw new NullPointerException();
			}
			if (type == null || displayName == null) {
				throw new NullPointerException(name);
			}
			theName = name;
			theType = type;
			theGroupName = groupName;
			theDisplayName = displayName;
			isInternalOnly = internalOnly;
			isUI = ui;
			theParameters = parameters;
			theCache = createCache(theParameters);

			hashCode = -1;
		}

		protected int compileHash() {
			int hash = MetricUtils.hashName(theName) * 29 + theType.hashCode() * 23;
			if (this instanceof RelationMetricType) {
				hash += ((RelationMetricType<?>) this).getTargetAnchorType().hashCode() * 19;
			} else if (this instanceof MultiRelationMetricType) {
				hash += ((MultiRelationMetricType<?, ?>) this).getTargetAnchorType().hashCode() * 19;
			}
			for (MetricParameter<?> param : theParameters.values()) {
				hash += param.hashCode();
			}
			return hash;
		}

		@Override
		public String getName() {
			return theName;
		}

		@Override
		public TypeToken<T> getType() {
			return theType;
		}

		@Override
		public String getGroupName() {
			return theGroupName;
		}

		@Override
		public String getDisplayName() {
			return theDisplayName;
		}

		@Override
		public boolean isInternalOnly() {
			return isInternalOnly;
		}

		@Override
		public boolean isUI() {
			return isUI;
		}

		@Override
		public Map<String, MetricParameter<?>> getParameters() {
			return theParameters;
		}

		@Override
		public Builder<T> build() {
			return build(null);
		}

		@Override
		public Builder<T> build(MetricParameterMap initParameters) {
			return MetricBuilder.build(this, initParameters);
		}

		@Override
		public MetricInstanceCache getCache() {
			return theCache;
		}

		@Override
		public int hashCode() {
			if (hashCode == -1) {
				hashCode = compileHash();
			}
			return hashCode;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			} else if (!(obj instanceof MetricType) || hashCode() != obj.hashCode()) {
				return false;
			} else if (this instanceof RelationMetricType) {
				if (!(obj instanceof RelationMetricType)) {
					return false;
				}
				RelationMetricType<?> rmtThis = (RelationMetricType<?>) this;
				RelationMetricType<?> rmtObj = (RelationMetricType<?>) obj;
				if (!rmtThis.getTargetAnchorType().equals(rmtObj.getTargetAnchorType())) {
					return false;
				}
			} else if (obj instanceof RelationMetricType) {
				return false;
			} else if (this instanceof MultiRelationMetricType) {
				if (!(obj instanceof MultiRelationMetricType)) {
					return false;
				}
				MultiRelationMetricType<?, ?> rmtThis = (MultiRelationMetricType<?, ?>) this;
				MultiRelationMetricType<?, ?> rmtObj = (MultiRelationMetricType<?, ?>) obj;
				if (!rmtThis.getTargetAnchorType().equals(rmtObj.getTargetAnchorType())) {
					return false;
				}
			} else if (obj instanceof MultiRelationMetricType) {
				return false;
			}
			MetricType<?> other = (MetricType<?>) obj;
			return theName.equals(other.getName()) && theType.equals(other.getType()) && theParameters.equals(other.getParameters());
		}

		@Override
		public String toString() {
			StringBuilder str = new StringBuilder(theName);
			str.append('(').append(theType);
			if (!theParameters.isEmpty()) {
				str.append(", ").append(theParameters.values());
			}
			str.append(')');
			return str.toString();
		}
	}

	private static class SimpleParameter<T> implements MetricParameter<T> {
		private final String theName;
		private final TypeToken<T> theType;
		private final boolean isNullable;
		private final Map<String, Predicate<? super T>> theFilters;
		private final List<T> theAllowedValues;
		private final Optional<T> theDefault;

		SimpleParameter(String name, TypeToken<T> type, boolean nullable, Map<String, Predicate<? super T>> filters, List<T> allowedValues,
			Optional<T> defaultValue) {
			theName = name;
			theType = type;
			isNullable = nullable;
			theFilters = filters.isEmpty() ? Collections.emptyMap() : Collections.unmodifiableMap(new LinkedHashMap<>(filters));
			if (allowedValues.isEmpty()) {
				List<T> enumValues = (List<T>) TypeTokens.get().getEnumValues(type);
				if (enumValues != null) {
					theAllowedValues = enumValues;
				} else {
					theAllowedValues = Collections.emptyList();
				}
			} else {
				List<T> copy = new ArrayList<>(allowedValues.size());
				copy.addAll(allowedValues);
				theAllowedValues = Collections.unmodifiableList(copy);
			}
			theDefault = defaultValue;
		}

		@Override
		public String getName() {
			return theName;
		}

		@Override
		public TypeToken<T> getType() {
			return theType;
		}

		@Override
		public boolean isNullable() {
			return isNullable;
		}

		@Override
		public Map<String, Predicate<? super T>> getFilters() {
			return theFilters;
		}

		@Override
		public List<T> getAllowedValues() {
			return theAllowedValues;
		}

		@Override
		public Optional<T> getDefault() {
			return theDefault;
		}

		@Override
		public int hashCode() {
			return MetricUtils.hashName(theName) * 13 + theType.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof MetricParameter)) {
				return false;
			}
			return theName.equals(((MetricParameter<?>) obj).getName()) && theType.equals(((MetricParameter<?>) obj).getType());
		}

		@Override
		public String toString() {
			return theName + "(" + theType + ")";
		}
	}

	private static class RelationSimpleMetricTypeImpl<A extends Anchor> extends MetricTypeImpl<A> implements RelationMetricType.Simple<A> {
		private final AnchorType<A> theTargetAnchorType;

		RelationSimpleMetricTypeImpl(String name, TypeToken<A> type, String groupName, String displayName, boolean internalOnly, boolean ui,
			CustomOrderedQuickMap<String, MetricParameter<?>> parameters, AnchorType<A> targetAnchorType) {
			super(name, type, groupName, displayName, internalOnly, ui, parameters);
			if (!RelationMetricType.isRelation(type)) {
				throw new IllegalArgumentException("Type " + type + " cannot be a relation");
			}
			theTargetAnchorType = targetAnchorType;
		}

		@Override
		public AnchorType<A> getTargetAnchorType() {
			return theTargetAnchorType;
		}
	}

	private static class MultiRelationSimpleMetricTypeImpl<A extends Anchor, C extends Collection<A>> extends MetricTypeImpl<C>
		implements MultiRelationMetricType.Simple<A, C> {
		private final AnchorType<A> theTargetAnchorType;

		MultiRelationSimpleMetricTypeImpl(String name, TypeToken<C> type, String groupName, String displayName, boolean internalOnly,
			boolean ui, CustomOrderedQuickMap<String, MetricParameter<?>> parameters, AnchorType<A> targetAnchorType) {
			super(name, type, groupName, displayName, internalOnly, ui, parameters);
			if (!MultiRelationMetricType.isMultiRelation(type)) {
				throw new IllegalArgumentException("Type " + type + " cannot be a multi-relation");
			}
			theTargetAnchorType = targetAnchorType;
		}

		@Override
		public AnchorType<A> getTargetAnchorType() {
			return theTargetAnchorType;
		}
	}

	private static class RelationParamImpl<P> implements RelationMetricParameter<P> {
		final String exposedName;
		final boolean isRelative;
		final MetricParameter<P> internParam;

		RelationParamImpl(String exposedName, boolean isRelative, MetricParameter<P> internParam) {
			this.exposedName = exposedName;
			this.isRelative = isRelative;
			this.internParam = internParam;
		}

		@Override
		public String getName() {
			return exposedName;
		}

		@Override
		public boolean isRelative() {
			return isRelative;
		}

		@Override
		public MetricParameter<P> getInternalParam() {
			return internParam;
		}

		@Override
		public int hashCode() {
			return MetricUtils.hashName(exposedName) * 13 + getType().hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof MetricParameter && exposedName.equals(((MetricParameter<?>) obj).getName())
				&& getType().equals(((MetricParameter<?>) obj).getType());
		}

		@Override
		public String toString() {
			return exposedName + "(" + getType() + ")";
		}
	}

	private static CustomOrderedQuickMap<String, RelationMetricParameter<?>> collectRelativeParameters(MetricType<?> relationMetric,
		MetricType<?> relativeMetric) {
		int cap = relationMetric.getParameters().size() + relativeMetric.getParameters().size();
		Map<String, RelationParamImpl<?>> params = new LinkedHashMap<>(cap);
		for (Map.Entry<String, ? extends MetricParameter<?>> param : relationMetric.getParameters().entrySet()) {
			String internName = param.getKey();
			String exposedName = internName;
			if (relativeMetric.getParameters().containsKey(exposedName)) {
				// If the relative metric defines a parameter with the same name, prepend the name of the relation metric
				// This is not bullet-proof, but I don't foresee relation metrics and actual metrics having same-name parameters anyway
				// But if we dive really deep with derived metrics, this may cause problems
				exposedName = relationMetric.getName() + "." + exposedName;
				if (relationMetric.getParameters().containsKey(exposedName) || relativeMetric.getParameters().containsKey(exposedName)) {
					throw new IllegalStateException("Parameter naming collision! " + relationMetric + "." + internName);
				}
			}
			RelationParamImpl<?> relParam = new RelationParamImpl<>(exposedName, false, (MetricParameter<Object>) param.getValue());
			params.put(exposedName, relParam);
		}
		for (Map.Entry<String, ? extends MetricParameter<?>> param : relativeMetric.getParameters().entrySet()) {
			String internName = param.getKey();
			String exposedName = internName;
			if (relationMetric.getParameters().containsKey(exposedName)) {
				exposedName = relativeMetric.getName() + "." + exposedName;
				if (relationMetric.getParameters().containsKey(exposedName) || relativeMetric.getParameters().containsKey(exposedName)) {
					throw new IllegalStateException("Parameter naming collision! " + relationMetric + "." + internName);
				}
			}
			RelationParamImpl<?> relParam = new RelationParamImpl<>(exposedName, true, (MetricParameter<Object>) param.getValue());
			params.put(exposedName, relParam);
		}
		CustomOrderedQuickMap<String, RelationMetricParameter<?>> collected = buildParameters(params, Collections.emptyList());
		if (collected.isEmpty() && (!relationMetric.getParameters().isEmpty() || !relativeMetric.getParameters().isEmpty())) {
			BreakpointHere.breakpoint();
		}
		return collected;
	}

	private static class RelatedMetricTypeImpl<T> implements RelatedMetricType<T>, CachingMetricType {
		private final RelationMetricType<? extends Anchor> theRelationMetric;
		private final MetricType<T> theRelativeMetric;
		private final CustomOrderedQuickMap<String, RelationMetricParameter<?>> theParameters;
		private final MetricInstanceCache theCache;
		private int hashCode;

		RelatedMetricTypeImpl(RelationMetricType<? extends Anchor> relationMetric, MetricType<T> relativeMetric) {
			theRelationMetric = relationMetric;
			theRelativeMetric = relativeMetric;
			theParameters = collectRelativeParameters(theRelationMetric, theRelativeMetric);
			theCache = createCache(theParameters);
			hashCode = -1;
		}

		@Override
		public Map<String, RelationMetricParameter<?>> getParameters() {
			return theParameters;
		}

		@Override
		public RelatedMetricType.Builder<T> build() {
			return build(null);
		}

		@Override
		public RelatedMetricType.Builder<T> build(MetricParameterMap initParameters) {
			return MetricBuilder.buildRelation(this, initParameters);
		}

		@Override
		public RelationMetricType<?> getRelationMetricType() {
			return theRelationMetric;
		}

		@Override
		public MetricType<T> getRelativeMetricType() {
			return theRelativeMetric;
		}

		@Override
		public MetricInstanceCache getCache() {
			return theCache;
		}

		@Override
		public int hashCode() {
			if (hashCode == -1) {
				hashCode = Objects.hash(theRelationMetric, theRelativeMetric);
			}
			return hashCode;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			} else if (!(obj instanceof RelatedMetricType) || hashCode() != obj.hashCode()) {
				return false;
			} else {
				return theRelationMetric.equals(((RelatedMetricType<?>) obj).getRelationMetricType())
					&& theRelativeMetric.equals(((RelatedMetricType<?>) obj).getRelativeMetricType());
			}
		}

		@Override
		public String toString() {
			return getName();
		}
	}

	private static class RelationRelatedMetricTypeImpl<A extends Anchor> extends RelatedMetricTypeImpl<A> implements RelationMetricType<A> {
		RelationRelatedMetricTypeImpl(RelationMetricType<? extends Anchor> relationMetric, RelationMetricType<A> relativeMetric) {
			super(relationMetric, relativeMetric);
		}

		@Override
		public RelationMetricType<A> getRelativeMetricType() {
			return (RelationMetricType<A>) super.getRelativeMetricType();
		}

		@Override
		public AnchorType<A> getTargetAnchorType() {
			return getRelativeMetricType().getTargetAnchorType();
		}
	}

	private static class MultiRelationRelatedMetricTypeImpl<A extends Anchor, C extends Collection<? extends A>>
		extends RelatedMetricTypeImpl<C> implements MultiRelationMetricType<A, C> {
		MultiRelationRelatedMetricTypeImpl(RelationMetricType<? extends Anchor> relationMetric,
			MultiRelationMetricType<A, C> relativeMetric) {
			super(relationMetric, relativeMetric);
		}

		@Override
		public MultiRelationMetricType<A, C> getRelativeMetricType() {
			return (MultiRelationMetricType<A, C>) super.getRelativeMetricType();
		}

		@Override
		public AnchorType<A> getTargetAnchorType() {
			return getRelativeMetricType().getTargetAnchorType();
		}
	}

	private static class AggregateMetricTypeImpl<T, X> implements AggregateMetricType<T, X>, CachingMetricType {
		private final MultiRelationMetricType<?, ?> theRelationMetric;
		private final MetricType<T> theRelativeMetric;
		private final MetricAggregation<? super T, X> theAggregation;
		private final CustomOrderedQuickMap<String, RelationMetricParameter<?>> theParameters;
		private final MetricInstanceCache theCache;
		private int hashCode;

		AggregateMetricTypeImpl(MultiRelationMetricType<?, ?> relationMetric, MetricType<T> relativeMetric,
			MetricAggregation<? super T, X> aggregation) {
			theRelationMetric = relationMetric;
			theRelativeMetric = relativeMetric;
			theAggregation = aggregation;
			theParameters = collectRelativeParameters(theRelationMetric, theRelativeMetric);
			theCache = createCache(theParameters);
			hashCode = -1;
		}

		@Override
		public Map<String, ? extends RelationMetricParameter<?>> getParameters() {
			return theParameters;
		}

		@Override
		public MultiRelationMetricType<?, ?> getRelationMetricType() {
			return theRelationMetric;
		}

		@Override
		public MetricType<T> getRelativeMetricType() {
			return theRelativeMetric;
		}

		@Override
		public MetricAggregation<? super T, X> getAggregation() {
			return theAggregation;
		}

		@Override
		public AggregateMetricType.Builder<T, X> build() {
			return build(null);
		}

		@Override
		public AggregateMetricType.Builder<T, X> build(MetricParameterMap initParameters) {
			return MetricBuilder.buildAggregate(this, initParameters);
		}

		@Override
		public MetricInstanceCache getCache() {
			return theCache;
		}

		@Override
		public int hashCode() {
			if (hashCode == -1) {
				hashCode = Objects.hash(theRelationMetric, theRelativeMetric, theAggregation);
			}
			return hashCode;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			} else if (!(obj instanceof AggregateMetricType) || hashCode() != obj.hashCode()) {
				return false;
			} else {
				return theRelationMetric.equals(((AggregateMetricType<?, ?>) obj).getRelationMetricType())
					&& theRelativeMetric.equals(((AggregateMetricType<?, ?>) obj).getRelativeMetricType())
					&& theAggregation.equals(((AggregateMetricType<?, ?>) obj).getAggregation());
			}
		}

		@Override
		public String toString() {
			return getName();
		}
	}

	private static class RelationAggregateMetricTypeImpl<T, A extends Anchor> extends AggregateMetricTypeImpl<T, A>
		implements RelationMetricType<A> {
		private final AnchorType<A> theTargetAnchorType;

		RelationAggregateMetricTypeImpl(MultiRelationMetricType<?, ?> relationMetric, MetricType<T> relativeMetric,
			AnchorType<A> targetAnchorType, MetricAggregation<? super T, A> aggregation) {
			super(relationMetric, relativeMetric, aggregation);
			theTargetAnchorType = targetAnchorType;
		}

		@Override
		public AnchorType<A> getTargetAnchorType() {
			return theTargetAnchorType;
		}
	}

	private static class MultiRelationAggregateMetricTypeImpl<T, A extends Anchor, C extends Collection<A>>
		extends AggregateMetricTypeImpl<T, C> implements MultiRelationMetricType<A, C> {
		private final AnchorType<A> theTargetAnchorType;

		MultiRelationAggregateMetricTypeImpl(MultiRelationMetricType<?, ?> relationMetric, MetricType<T> relativeMetric,
			AnchorType<A> targetAnchorType, MetricAggregation<? super T, C> aggregation) {
			super(relationMetric, relativeMetric, aggregation);
			theTargetAnchorType = targetAnchorType;
		}

		@Override
		public AnchorType<A> getTargetAnchorType() {
			return theTargetAnchorType;
		}
	}

	private static class SingletonMetricCache implements MetricInstanceCache {
		private MetricParameterMap theMetric;

		@Override
		public <X extends MetricParameterMap> X get(QuickMap<String, MetricParameterValue<?>> parameters,
			Function<QuickMap<String, MetricParameterValue<?>>, X> builder) {
			if (theMetric == null)
				theMetric = builder.apply(QuickMap.empty());
			return (X) theMetric;
		}
	}

	private static class ExhaustiveMetricCache implements MetricInstanceCache {
		private final Map<ParamValueSet, MetricParameterMap> theMetricsByParam;

		ExhaustiveMetricCache(int possibleCombinations) {
			// This should be thread-safe if the map doesn't have to re-build the whole table
			theMetricsByParam = new HashMap<>(possibleCombinations * 2);
		}

		@Override
		public <X extends MetricParameterMap> X get(QuickMap<String, MetricParameterValue<?>> parameters,
			Function<QuickMap<String, MetricParameterValue<?>>, X> builder) {
			return (X) theMetricsByParam.computeIfAbsent(new ParamValueSet(parameters), pvs -> builder.apply(pvs.theParams));
		}

		private static class ParamValueSet {
			private final int[] valueIndexes;
			final QuickMap<String, MetricParameterValue<?>> theParams;
			private final int hashCode;

			ParamValueSet(QuickMap<String, MetricParameterValue<?>> params) {
				theParams = params;
				valueIndexes = new int[params.keySet().size()];
				for (int i = 0; i < valueIndexes.length; i++) {
					MetricParameterValue<?> pv = params.get(i);
					if (pv == null) {
						valueIndexes[i] = -1;
					} else {
						valueIndexes[i] = pv.getEnumIndex();
					}
				}
				hashCode = Arrays.hashCode(valueIndexes);
			}

			@Override
			public int hashCode() {
				return hashCode;
			}

			@Override
			public boolean equals(Object obj) {
				if (this == obj) {
					return true;
				} else if (!(obj instanceof ParamValueSet)) {
					return false;
				}
				ParamValueSet other = (ParamValueSet) obj;
				return Arrays.equals(valueIndexes, other.valueIndexes);
			}

			@Override
			public String toString() {
				return theParams.toString();
			}
		}
	}
}
