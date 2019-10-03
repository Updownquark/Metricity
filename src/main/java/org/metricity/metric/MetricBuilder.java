package org.metricity.metric;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

import org.metricity.anchor.Anchor;
import org.metricity.metric.MetricParameterSet.MetricParameterMap;
import org.metricity.metric.MetricTypeBuilder.MetricInstanceCache;
import org.metricity.metric.RelatedMetric.RelationMetricParameterValue;
import org.metricity.metric.RelatedMetricType.RelationMetricParameter;
import org.metricity.metric.util.MetricUtils;
import org.observe.util.TypeTokens;
import org.qommons.BreakpointHere;
import org.qommons.StringUtils;
import org.qommons.collect.QuickSet;
import org.qommons.collect.QuickSet.CustomOrderedQuickMap;
import org.qommons.collect.QuickSet.QuickMap;

class MetricBuilder {
	static MetricParameterSet.Builder buildParams(MetricParameterSet paramSet, MetricParameterMap initParameters) {
		return new ParamMapBuilder(paramSet, initParameters);
	}

	static <T> SimpleMetricType.Builder<T> build(SimpleMetricType<T> type, MetricParameterMap initParameters) {
		return new MetricBuilderImpl<>(type, initParameters);
	}

	static <T> RelatedMetricType.Builder<T> buildRelation(RelatedMetricType<T> type, MetricParameterMap initParameters) {
		return new RelatedMetricBuilder<>(type, initParameters);
	}

	static <T, X> AggregateMetricType.Builder<T, X> buildAggregate(AggregateMetricType<T, X> type, MetricParameterMap initParameters) {
		return new AggregateMetricBuilder<>(type, initParameters);
	}

	private interface ParamSetBackedBuilder {
		void stealParameter(int parameterIndex, CustomOrderedQuickMap<String, ? extends MetricParameterValue<?>> parameters)
			throws IllegalArgumentException;
	}

	private interface ParamSetBackedParamMap {
		CustomOrderedQuickMap<String, ? extends MetricParameterValue<?>> getParamSetParameters();
	}

	private static class ParamMapBuilder implements MetricParameterSet.Builder, ParamSetBackedBuilder {
		private final MetricParameterSet theType;
		private boolean used;
		private QuickMap<String, MetricParameterValue<?>> theParameters;

		ParamMapBuilder(MetricParameterSet type, MetricParameterMap initParameters) {
			theType = type;
			theParameters = createParamValues(theType);
			if (initParameters != null) {
				CustomOrderedQuickMap<String, ? extends MetricParameterValue<?>> copm;
				if (initParameters instanceof ParamSetBackedParamMap//
					&& (copm = ((ParamSetBackedParamMap) initParameters).getParamSetParameters()).getMap().keySet()
						.equals(theParameters.keySet())) {
					for (int i = 0; i < theParameters.keySet().size(); i++) {
						MetricParameterValue<?> otherParam = copm.getMap().get(i);
						MetricParameter<?> param = getParamType(theType, theParameters.keySet(), i);
						if (param == otherParam.getParameter()) {
							theParameters.put(i, otherParam);
						} else if (MetricUtils.getParamEnumValues(param) == MetricUtils.getParamEnumValues(otherParam.getParameter())) {
							_withParameter(param.getName(), i, otherParam.getValue(), otherParam.getEnumIndex());
						} else {
							_withParameter(param.getName(), i, otherParam.getValue(), -1);
						}
					}
				} else {
					for (int i = 0; i < theParameters.keySet().size(); i++) {
						MetricParameterValue<?> otherParam = initParameters.getParameters().get(theParameters.keySet().get(i));
						if (otherParam != null && otherParam.getValue() != null) {
							MetricParameter<?> param = getParamType(theType, theParameters.keySet(), i);
							if (TypeTokens.get().isInstance(param.getType(), otherParam.getValue())) {
								if (MetricUtils.getParamEnumValues(param) == MetricUtils.getParamEnumValues(otherParam.getParameter())) {
									_withParameter(param.getName(), i, otherParam.getValue(), otherParam.getEnumIndex());
								} else {
									_withParameter(param.getName(), i, otherParam.getValue(), -1);
								}
							}
						}
					}
				}
			}
		}

		@Override
		public MetricParameterSet getType() {
			return theType;
		}

		@Override
		public MetricParameterSet.Builder with(String parameterName, Object value) {
			_withParameter(parameterName, theParameters.keyIndex(parameterName), value, -1);
			return this;
		}

		@Override
		public void stealParameter(int parameterIndex, CustomOrderedQuickMap<String, ? extends MetricParameterValue<?>> parameters)
			throws IllegalArgumentException {
			MetricParameterValue<?> otherParam = parameters.getMap().get(parameterIndex);
			String parameterName = otherParam.getParameter().getName();
			int keyIndex;
			if (parameters.getMap().keySet() == theParameters.keySet()) {
				keyIndex = parameterIndex;
			} else {
				keyIndex = theParameters.keySet().indexOf(parameterName);
				if (keyIndex < 0) {
					return;
				}
			}
			MetricParameter<?> param = getParamType(theType, theParameters.keySet(), keyIndex);
			if (param == otherParam.getParameter()) {
				// Just transfer the parameter value instance
				theParameters.put(keyIndex, otherParam);
			} else if (MetricUtils.getParamEnumValues(param) == MetricUtils.getParamEnumValues(otherParam.getParameter())) {
				_withParameter(parameterName, keyIndex, otherParam.getValue(), otherParam.getEnumIndex());
			} else {
				_withParameter(parameterName, keyIndex, otherParam.getValue(), -1);
			}
		}

		private <P> void _withParameter(String parameterName, int paramIndex, P value, int enumIndex) {
			if (used) {
				throw new IllegalArgumentException("This builder has been used");
			} else if (paramIndex < 0) {
				throw new IllegalArgumentException("No such parameter " + theType.getName() + "." + parameterName);
			}
			MetricParameter<P> param = (MetricParameter<P>) getParamType(theType, theParameters.keySet(), paramIndex);
			if (param == null) {
				throw new IllegalArgumentException("No such parameter " + theType.getName() + "." + parameterName);
			} else if (value == null && !param.isNullable()) {
				throw new IllegalArgumentException("Parameter " + theType.getName() + "." + param + " cannot be null");
			} else if (value != null) {
				if (!TypeTokens.get().isInstance(param.getType(), value)) {
					throw new IllegalArgumentException("Value " + value + ", type " + value.getClass().getSimpleName()
						+ " cannot be assigned to parameter " + theType.getName() + "." + param);
				}
				String filtered = param.filter(value);
				if (filtered != null) {
					throw new IllegalArgumentException(filtered);
				}
			}

			theParameters.put(paramIndex, new MetricParamValueImpl<>(param, value, enumIndex));
		}

		protected QuickMap<String, MetricParameterValue<?>> checkParameters() {
			if (used) {
				throw new IllegalArgumentException("This builder has been used");
			}
			used = true;
			if (theType.getParameters().isEmpty()) {
				return theParameters;
			}
			Set<String> missing = null;
			for (int i = 0; i < theParameters.keySet().size(); i++) {
				if (theParameters.get(i) != null) {
					continue;
				}
				MetricParameter<?> param = getParamType(theType, theParameters.keySet(), i);
				if (param.getDefault().isPresent()) {
					MetricParameterValue<?> pv = new MetricParamValueImpl<>((MetricParameter<Object>) param, param.getDefault().get(), i);
					theParameters.put(i, pv);
					continue;
				}
				if (missing == null) {
					missing = new TreeSet<>();
				}
				missing.add(param.getName());
			}
			if (missing != null) {
				throw new IllegalStateException("Missing parameters " + missing);
			}
			return theParameters;
		}

		@Override
		public MetricParameterMap build() {
			MetricParameterMap metric = buildCached(theType, checkParameters(), this::create);
			return metric;
		}

		protected MetricParameterMap create(QuickMap<String, MetricParameterValue<?>> params) {
			return new ParameterMapImpl(theType, params);
		}
	}

	private static class MetricBuilderImpl<T> extends ParamMapBuilder implements SimpleMetricType.Builder<T> {
		MetricBuilderImpl(SimpleMetricType<T> type, MetricParameterMap initParameters) {
			super(type, initParameters);
		}

		@Override
		public SimpleMetricType<T> getType() {
			return (SimpleMetricType<T>) super.getType();
		}

		@Override
		public SimpleMetricType.Builder<T> with(String parameterName, Object value) {
			super.with(parameterName, value);
			return this;
		}

		@Override
		public SimpleMetric<T> build() {
			SimpleMetric<T> metric = buildCached(super.getType(), checkParameters(), this::create);
			return metric;
		}

		@Override
		protected SimpleMetric<T> create(QuickMap<String, MetricParameterValue<?>> params) {
			return new MetricImpl<>(getType(), params);
		}
	}

	private static QuickMap<String, MetricParameterValue<?>> createParamValues(MetricParameterSet metricType) {
		if (metricType.getParameters() instanceof CustomOrderedQuickMap) {
			return ((CustomOrderedQuickMap<String, ?>) metricType.getParameters()).getMap().keySet().createMap();
		} else {
			return new QuickSet<>(StringUtils.DISTINCT_NUMBER_TOLERANT, metricType.getParameters().keySet()).createMap();
		}
	}

	private static MetricParameter<?> getParamType(MetricParameterSet type, QuickSet<String> keySet, int keyIndex) {
		if (type.getParameters() instanceof CustomOrderedQuickMap) {
			return ((CustomOrderedQuickMap<String, ? extends MetricParameter<?>>) type.getParameters()).getMap().get(keyIndex);
		} else {
			return type.getParameters().get(keySet.get(keyIndex));
		}
	}

	private static <X extends MetricParameterMap> X buildCached(MetricParameterSet type, QuickMap<String, MetricParameterValue<?>> params,
		Function<QuickMap<String, MetricParameterValue<?>>, X> builder) {
		MetricInstanceCache cache = null;
		if (type instanceof MetricTypeBuilder.CachingMetricType) {
			cache = ((MetricTypeBuilder.CachingMetricType) type).getCache();
		}
		if (cache != null) {
			return cache.get(params, builder);
		} else {
			return builder.apply(params);
		}
	}

	private static class ParameterMapImpl implements MetricParameterMap {
		private final MetricParameterSet theType;
		private final CustomOrderedQuickMap<String, MetricParameterValue<?>> theParameters;

		ParameterMapImpl(MetricParameterSet type, QuickMap<String, MetricParameterValue<?>> parameters) {
			theType = type;
			CustomOrderedQuickMap<String, ? extends MetricParameter<?>> ssTypes = (CustomOrderedQuickMap<String, ? extends MetricParameter<?>>) type
				.getParameters();
			theParameters = new CustomOrderedQuickMap<>(parameters, ssTypes.keySet());
		}

		@Override
		public MetricParameterSet getType() {
			return theType;
		}

		@Override
		public Map<String, ? extends MetricParameterValue<?>> getParameters() {
			return theParameters;
		}

		@Override
		public int hashCode() {
			int hash = theType.hashCode() * 131;
			for (MetricParameterValue<?> param : theParameters.values()) {
				hash += param.getParameter().getName().hashCode() * 17 + Objects.hashCode(param.getValue());
			}
			return hash;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			} else if (!(obj instanceof MetricParameterMap)) {
				return false;
			}
			MetricParameterMap other = (MetricParameterMap) obj;
			if (!theType.equals(other.getType())) {
				return false;
			}
			Map<String, ? extends MetricParameterValue<?>> params = other.getParameters();
			if (params instanceof CustomOrderedQuickMap) {
				CustomOrderedQuickMap<String, ? extends MetricParameterValue<?>> otherParams = (CustomOrderedQuickMap<String, ? extends MetricParameterValue<?>>) params;
				for (int i = 0; i < theParameters.getMap().keySet().size(); i++) {
					MetricParameterValue<?> pv = theParameters.getMap().get(i);
					MetricParameterValue<?> otherPV = otherParams.getMap().get(i);
					if (!Objects.equals(pv == null ? null : pv.getValue(), otherPV == null ? null : otherPV.getValue())) {
						return false;
					}
				}
			} else {
				for (int i = 0; i < theParameters.getMap().keySet().size(); i++) {
					MetricParameterValue<?> pv = theParameters.getMap().get(i);
					MetricParameterValue<?> otherPV = params.get(theParameters.getMap().keySet().get(i));
					if (!Objects.equals(pv == null ? null : pv.getValue(), otherPV == null ? null : otherPV.getValue())) {
						return false;
					}
				}
			}
			return true;
		}

		@Override
		public String toString() {
			StringBuilder str = new StringBuilder(theType.getName());
			if (!theParameters.isEmpty()) {
				str.append(theParameters.values());
			}
			return str.toString();
		}
	}

	private static class MetricImpl<T> implements SimpleMetric<T> {
		private final SimpleMetricType<T> theType;
		private final CustomOrderedQuickMap<String, MetricParameterValue<?>> theParameters;
		protected final int hashCode;

		MetricImpl(SimpleMetricType<T> type, QuickMap<String, MetricParameterValue<?>> parameters) {
			theType = type;
			CustomOrderedQuickMap<String, ? extends MetricParameter<?>> ssTypes = (CustomOrderedQuickMap<String, ? extends MetricParameter<?>>) type
				.getParameters();
			theParameters = new CustomOrderedQuickMap<>(parameters, ssTypes.keySet());
			hashCode = compileHash();
		}

		protected int compileHash() {
			int hash = theType.hashCode() * 131;
			for (MetricParameterValue<?> param : theParameters.values()) {
				hash += param.getParameter().getName().hashCode() * 17 + Objects.hashCode(param.getValue());
			}
			return hash;
		}

		@Override
		public SimpleMetricType<T> getType() {
			return theType;
		}

		@Override
		public Map<String, ? extends MetricParameterValue<?>> getParameters() {
			return theParameters;
		}

		@Override
		public int hashCode() {
			return hashCode;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			} else if (!(o instanceof Metric) || hashCode != o.hashCode()) {
				return false;
			} else if (!theType.equals(((Metric<?>) o).getType())) {
				return false;
			} else {
				Map<String, ? extends MetricParameterValue<?>> params = ((Metric<?>) o).getParameters();
				if (params instanceof CustomOrderedQuickMap) {
					CustomOrderedQuickMap<String, ? extends MetricParameterValue<?>> otherParams = (CustomOrderedQuickMap<String, ? extends MetricParameterValue<?>>) params;
					for (int i = 0; i < theParameters.getMap().keySet().size(); i++) {
						MetricParameterValue<?> pv = theParameters.getMap().get(i);
						MetricParameterValue<?> otherPV = otherParams.getMap().get(i);
						if (!Objects.equals(pv == null ? null : pv.getValue(), otherPV == null ? null : otherPV.getValue())) {
							return false;
						}
					}
				} else {
					for (int i = 0; i < theParameters.getMap().keySet().size(); i++) {
						MetricParameterValue<?> pv = theParameters.getMap().get(i);
						MetricParameterValue<?> otherPV = params.get(theParameters.getMap().keySet().get(i));
						if (!Objects.equals(pv == null ? null : pv.getValue(), otherPV == null ? null : otherPV.getValue())) {
							return false;
						}
					}
				}
			}
			return true;
		}

		@Override
		public String toString() {
			StringBuilder str = new StringBuilder(theType.getDisplayName());
			if (!theParameters.isEmpty()) {
				str.append(theParameters.values());
			}
			return str.toString();
		}
	}

	private static class MetricParamValueImpl<P> implements MetricParameterValue<P> {
		private final MetricParameter<P> theParameter;
		private final P theValue;
		private final int theEnumIndex;

		MetricParamValueImpl(MetricParameter<P> parameter, P value, int enumIndex) {
			if (parameter == null) {
				BreakpointHere.breakpoint();
			}
			theParameter = parameter;
			theValue = value;
			if (enumIndex >= 0) {
				theEnumIndex = enumIndex;
			} else {
				List<P> enumValues = MetricUtils.getParamEnumValues(parameter);
				if (enumValues == null) {
					theEnumIndex = -1;
				} else {
					theEnumIndex = enumValues.indexOf(value);
				}
			}
		}

		@Override
		public MetricParameter<P> getParameter() {
			return theParameter;
		}

		@Override
		public int getEnumIndex() {
			return theEnumIndex;
		}

		@Override
		public P getValue() {
			return theValue;
		}

		@Override
		public int hashCode() {
			return theParameter.hashCode() * 7 + Objects.hashCode(theValue);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			} else if (!(o instanceof MetricParameterValue)) {
				return false;
			} else {
				return theParameter.equals(((MetricParameterValue<?>) o).getParameter())
					&& Objects.equals(theValue, ((MetricParameterValue<?>) o).getValue());
			}
		}

		@Override
		public String toString() {
			return new StringBuilder().append(theParameter.getName()).append('=').append(theValue).toString();
		}
	}

	private static abstract class RelationalMetricBuilder<T> implements MetricType.Builder<T>, ParamSetBackedBuilder {
		private final MetricType<T> theType;
		private MetricType.Builder<?> theRelationBuilder;
		private MetricType.Builder<?> theRelativeBuilder;
		private final QuickMap<String, MetricParameterValue<?>> theParameters;

		private boolean used;

		RelationalMetricBuilder(MetricType<T> type, MetricType.Builder<?> relationBuilder, MetricType.Builder<?> relativeBuilder,
			MetricParameterMap initParameters) {
			theType = type;
			theRelationBuilder = relationBuilder;
			theRelativeBuilder = relativeBuilder;
			theParameters = createParamValues(theType);

			if (initParameters != null) {
				// Even though both builders passed in here have already been initialized with initParameters,
				// we have to do this step as well for possible name-differentiated conflicting parameters in the given parameter map
				for (Map.Entry<String, ? extends MetricParameterValue<?>> otherParam : initParameters.getParameters().entrySet()) {
					if (otherParam.getValue() instanceof RelatedMetric.RelationMetricParameterValue) {
						RelatedMetric.RelationMetricParameterValue<?> rmp = (RelationMetricParameterValue<?>) otherParam.getValue();
						String paramName = rmp.getParameter().getInternalParam().getName();
						if (rmp.getParameter().getName() != paramName) {
							MetricType.Builder<?> rBuilder = rmp.getParameter().isRelative() ? theRelativeBuilder : theRelationBuilder;
							MetricParameter<?> param = rBuilder.getType().getParameter(paramName);
							if (param != null && TypeTokens.get().isInstance(param.getType(), rmp.getValue())) {
								rBuilder.with(paramName, rmp.getValue());
							}
						}
					}
				}
			}
		}

		@Override
		public MetricType<T> getType() {
			return theType;
		}

		MetricType.Builder<?> getRelationBuilder() {
			return theRelationBuilder;
		}

		MetricType.Builder<?> getRelativeBuilder() {
			return theRelativeBuilder;
		}

		@SuppressWarnings("unused")
		QuickMap<String, MetricParameterValue<?>> getParameters() {
			return theParameters;
		}

		@Override
		public MetricType.Builder<T> with(String parameterName, Object value) throws IllegalArgumentException {
			if (used) {
				throw new IllegalStateException("This builder is already used");
			}
			int keyIndex = theParameters.keySet().indexOf(parameterName);
			if (keyIndex < 0) {
				// May be a conflicting parameter. Call both sub-builders and let one throw an exception if needed.
				try {
					theRelationBuilder.with(parameterName, value);
				} catch (IllegalArgumentException e) {
					if (e.getMessage().startsWith("No such ")) {
						throw new IllegalArgumentException("No such parameter " + theType + "." + parameterName);
					} else {
						throw e;
					}
				}
				theRelativeBuilder.with(parameterName, value);
			} else {
				RelationMetricParameter<?> param = (RelationMetricParameter<Object>) getParamType(theType, theParameters.keySet(),
					keyIndex);
				if (param == null) {
					throw new IllegalArgumentException("No such parameter " + parameterName);
				}
				_with(param, keyIndex, value, -1);
			}
			return this;
		}

		@Override
		public void stealParameter(int parameterIndex, CustomOrderedQuickMap<String, ? extends MetricParameterValue<?>> parameters)
			throws IllegalArgumentException {
			if (used) {
				throw new IllegalStateException("This builder is already used");
			}
			MetricParameterValue<?> otherParam = parameters.getMap().get(parameterIndex);
			String parameterName = otherParam.getParameter().getName();
			int keyIndex;
			if (parameters.getMap().keySet() == theParameters.keySet()) {
				keyIndex = parameterIndex;
			} else {
				keyIndex = theParameters.keySet().indexOf(parameterName);
				if (keyIndex < 0) {
					// May be a conflicting parameter. Call both sub-builders.
					if (theRelationBuilder instanceof ParamSetBackedBuilder) {
						((ParamSetBackedBuilder) theRelationBuilder).stealParameter(parameterIndex, parameters);
					} else if (theRelationBuilder.getType().getParameters().containsKey(parameterName)) {
						theRelationBuilder.with(parameterName, parameters.getAt(parameterIndex).getValue());
					}
					if (theRelativeBuilder instanceof ParamSetBackedBuilder) {
						((ParamSetBackedBuilder) theRelativeBuilder).stealParameter(parameterIndex, parameters);
					} else if (theRelativeBuilder.getType().getParameters().containsKey(parameterName)) {
						theRelativeBuilder.with(parameterName, parameters.getAt(parameterIndex).getValue());
					}
					return;
				}
			}
			RelationMetricParameter<?> param = (RelationMetricParameter<?>) getParamType(theType, theParameters.keySet(), keyIndex);
			if (param.isRelative()) {
				if (theRelativeBuilder instanceof ParamSetBackedBuilder) {
					((ParamSetBackedBuilder) theRelativeBuilder).stealParameter(parameterIndex, parameters);
				} else {
					theRelativeBuilder.with(parameterName, parameters.getAt(parameterIndex).getValue());
				}
			} else {
				if (theRelationBuilder instanceof ParamSetBackedBuilder) {
					((ParamSetBackedBuilder) theRelationBuilder).stealParameter(parameterIndex, parameters);
				} else {
					theRelationBuilder.with(parameterName, parameters.getAt(parameterIndex).getValue());
				}
			}
		}

		private void _with(RelationMetricParameter<?> param, int keyIndex, Object value, int enumIndex) throws IllegalArgumentException {
			theParameters.put(keyIndex, new MetricParamValueImpl<>((RelationMetricParameter<Object>) param, value, -1));
			if (param.isRelative()) {
				theRelativeBuilder.with(param.getInternalParam().getName(), value);
			} else {
				theRelationBuilder.with(param.getInternalParam().getName(), value);
			}
		}

		@Override
		public Metric<T> build() {
			if (used) {
				throw new IllegalStateException("This builder is already used");
			}
			used = true;
			Metric<T> metric = buildCached(theType, theParameters, params -> create());
			theParameters.release();
			return metric;
		}

		protected abstract Metric<T> create();
	}

	private static class RelatedMetricBuilder<T> extends RelationalMetricBuilder<T> implements RelatedMetricType.Builder<T> {
		RelatedMetricBuilder(RelatedMetricType<T> type, MetricParameterMap initParameters) {
			super(type, type.getRelationMetricType().build(initParameters), type.getRelativeMetricType().build(initParameters),
				initParameters);
		}

		@Override
		public RelatedMetricType<T> getType() {
			return (RelatedMetricType<T>) super.getType();
		}

		@Override
		MetricType.Builder<? extends Anchor> getRelationBuilder() {
			return (MetricType.Builder<? extends Anchor>) super.getRelationBuilder();
		}

		@Override
		MetricType.Builder<T> getRelativeBuilder() {
			return (MetricType.Builder<T>) super.getRelativeBuilder();
		}

		@Override
		public RelatedMetricType.Builder<T> with(String parameterName, Object value) throws IllegalArgumentException {
			super.with(parameterName, value);
			return this;
		}

		@Override
		public RelatedMetric<T> build() {
			return (RelatedMetric<T>) super.build();
		}

		@Override
		protected RelatedMetric<T> create() {
			return new RelatedMetricImpl<>(getType(), getRelationBuilder().build(), getRelativeBuilder().build());
		}
	}

	private static class RelParamValueImpl<P> implements RelationMetricParameterValue<P> {
		private final RelationMetricParameter<P> theParameter;
		private final MetricParameterValue<P> theValue;

		RelParamValueImpl(RelationMetricParameter<P> parameter, MetricParameterValue<P> value) {
			theParameter = parameter;
			theValue = value;
		}

		@Override
		public RelationMetricParameter<P> getParameter() {
			return theParameter;
		}

		@Override
		public MetricParameterValue<P> getInternalParam() {
			return theValue;
		}

		@Override
		public P getValue() {
			return theValue.getValue();
		}

		@Override
		public int getEnumIndex() {
			return theValue.getEnumIndex();
		}

		@Override
		public int hashCode() {
			return theParameter.hashCode() * 7 + Objects.hashCode(theValue);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			} else if (!(o instanceof MetricParameterValue)) {
				return false;
			} else {
				return theParameter.equals(((MetricParameterValue<?>) o).getParameter())
					&& Objects.equals(theValue.getValue(), ((MetricParameterValue<?>) o).getValue());
			}
		}

		@Override
		public String toString() {
			return new StringBuilder().append(theParameter.getName()).append('=').append(theValue).toString();
		}
	}

	private static CustomOrderedQuickMap<String, ? extends RelationMetricParameterValue<?>> collectParams(
		Map<String, ? extends RelationMetricParameter<?>> paramTypes, Metric<?> relationMetric, Metric<?> relativeMetric) {
		if (paramTypes instanceof QuickSet.CustomOrderedQuickMap) {
			CustomOrderedQuickMap<String, ? extends RelationMetricParameter<?>> ssTypes = (CustomOrderedQuickMap<String, ? extends RelationMetricParameter<?>>) paramTypes;
			QuickMap<String, RelationMetricParameterValue<?>> pvs = ssTypes.getMap().keySet().createMap();
			for (int i = 0; i < pvs.keySet().size(); i++) {
				RelationMetricParameter<?> p = ssTypes.getMap().get(i);
				MetricParameterValue<?> value = (p.isRelative() ? relativeMetric : relationMetric).getParameters()
					.get(p.getInternalParam().getName());
				RelParamValueImpl<?> pv = new RelParamValueImpl<>((RelationMetricParameter<Object>) p,
					(MetricParameterValue<Object>) value);
				pvs.put(i, pv);
			}
			return new CustomOrderedQuickMap<>(pvs, ssTypes.keySet());
		} else {
			QuickSet<String> keySet = new QuickSet<>(StringUtils.DISTINCT_NUMBER_TOLERANT, paramTypes.keySet());
			QuickMap<String, RelationMetricParameterValue<?>> pvs = keySet.createMap();
			for (int i = 0; i < pvs.keySet().size(); i++) {
				RelationMetricParameter<?> p = paramTypes.get(keySet.get(i));
				MetricParameterValue<?> value = (p.isRelative() ? relativeMetric : relationMetric).getParameters()
					.get(p.getInternalParam().getName());
				RelParamValueImpl<?> pv = new RelParamValueImpl<>((RelationMetricParameter<Object>) p,
					(MetricParameterValue<Object>) value);
				pvs.put(i, pv);
			}
			return new CustomOrderedQuickMap<>(pvs, new QuickSet.CustomOrderedQuickSet<>(keySet, paramTypes.keySet()));
		}
	}

	private static class RelatedMetricImpl<T> implements RelatedMetric<T> {
		private final RelatedMetricType<T> theType;
		private final Metric<? extends Anchor> theRelationMetric;
		private final Metric<T> theRelativeMetric;
		private final Map<String, ? extends RelationMetricParameterValue<?>> theParameters;

		RelatedMetricImpl(RelatedMetricType<T> type, Metric<? extends Anchor> relationMetric, Metric<T> relativeMetric) {
			theType = type;
			theRelationMetric = relationMetric;
			theRelativeMetric = relativeMetric;
			theParameters = collectParams(type.getParameters(), relationMetric, relativeMetric);
		}

		@Override
		public Metric<? extends Anchor> getRelationMetric() {
			return theRelationMetric;
		}

		@Override
		public Metric<T> getRelativeMetric() {
			return theRelativeMetric;
		}

		@Override
		public RelatedMetricType<T> getType() {
			return theType;
		}

		@Override
		public Map<String, ? extends RelationMetricParameterValue<?>> getParameters() {
			return theParameters;
		}

		@Override
		public int hashCode() {
			return theRelationMetric.hashCode() * 13 + theRelativeMetric.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof RelatedMetric && theRelationMetric.equals(((RelatedMetric<?>) obj).getRelationMetric())
				&& theRelativeMetric.equals(((RelatedMetric<?>) obj).getRelativeMetric());
		}

		@Override
		public String toString() {
			return theRelationMetric + "." + theRelativeMetric;
		}
	}

	private static class AggregateMetricBuilder<T, X> extends RelationalMetricBuilder<X> implements AggregateMetricType.Builder<T, X> {
		AggregateMetricBuilder(AggregateMetricType<T, X> type, MetricParameterMap initParameters) {
			super(type, type.getRelationMetricType().build(initParameters), type.getRelativeMetricType().build(initParameters),
				initParameters);
		}

		@Override
		public AggregateMetricType<T, X> getType() {
			return (AggregateMetricType<T, X>) super.getType();
		}

		@Override
		MetricType.Builder<? extends Collection<? extends Anchor>> getRelationBuilder() {
			return (MetricType.Builder<? extends Collection<? extends Anchor>>) super.getRelationBuilder();
		}

		@Override
		MetricType.Builder<T> getRelativeBuilder() {
			return (MetricType.Builder<T>) super.getRelativeBuilder();
		}

		@Override
		public AggregateMetricType.Builder<T, X> with(String parameterName, Object value) throws IllegalArgumentException {
			super.with(parameterName, value);
			return this;
		}

		@Override
		public AggregateMetric<T, X> build() {
			return (AggregateMetric<T, X>) super.build();
		}

		@Override
		protected AggregateMetric<T, X> create() {
			return new AggregateMetricImpl<>(getType(), getRelationBuilder().build(), getRelativeBuilder().build());
		}
	}

	private static class AggregateMetricImpl<T, X> implements AggregateMetric<T, X> {
		private final AggregateMetricType<T, X> theType;
		private final Metric<? extends Collection<? extends Anchor>> theRelationMetric;
		private final Metric<T> theRelativeMetric;
		private final CustomOrderedQuickMap<String, ? extends RelationMetricParameterValue<?>> theParameters;

		AggregateMetricImpl(AggregateMetricType<T, X> type, Metric<? extends Collection<? extends Anchor>> relationMetric,
			Metric<T> relativeMetric) {
			theType = type;
			theRelationMetric = relationMetric;
			theRelativeMetric = relativeMetric;
			theParameters = collectParams(type.getParameters(), relationMetric, relativeMetric);
		}

		@Override
		public AggregateMetricType<T, X> getType() {
			return theType;
		}

		@Override
		public Map<String, ? extends MetricParameterValue<?>> getParameters() {
			return theParameters;
		}

		@Override
		public Metric<? extends Collection<? extends Anchor>> getRelationMetric() {
			return theRelationMetric;
		}

		@Override
		public Metric<T> getRelativeMetric() {
			return theRelativeMetric;
		}

		@Override
		public int hashCode() {
			return theType.hashCode() * 13 + theRelationMetric.hashCode() * 7 + theRelativeMetric.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			} else if (!(obj instanceof AggregateMetric)) {
				return false;
			}
			AggregateMetric<?, ?> other = (AggregateMetric<?, ?>) obj;
			if (!theType.equals(other.getType())) {
				return false;
			}
			return theRelationMetric.equals(other.getRelationMetric()) && theRelativeMetric.equals(other.getRelativeMetric());
		}

		@Override
		public String toString() {
			return theType.getAggregation() + "(" + theRelationMetric + "." + theRelativeMetric + ")";
		}
	}

}
