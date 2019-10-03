package org.metricity.metric;

import java.util.Map;

import org.qommons.Named;

/** A type of something with a set of metric parameter types. Typically a metric type, but may be a metric type precursor. */
public interface MetricParameterSet extends Named {
	/** Builds a {@link MetricParameterMap} */
	interface Builder {
		/** @return The parameter set that this builder creates instances for */
		MetricParameterSet getType();

		/**
		 * Supplies the value of a parameter
		 * 
		 * @param parameterName
		 *            The name of the parameter
		 * @param value
		 *            The value for the parameter
		 * @return This builder
		 * @throws IllegalArgumentException
		 *             If no parameter with the given name exists in this set or if the given value is not valid for the specified parameter
		 */
		Builder with(String parameterName, Object value) throws IllegalArgumentException;

		/** @return An instance of this builder's {@link #getType() type} */
		MetricParameterMap build();
	}

	/** @return The set of parameters in this set, by name */
	Map<String, ? extends MetricParameter<?>> getParameters();

	/**
	 * @param name
	 *            the name of the parameter to get
	 * @return The parameter in this set with the given name
	 * @throws IllegalArgumentException
	 *             If no parameter with the given name exists in this set
	 */
	default MetricParameter<?> getParameter(String name) throws IllegalArgumentException {
		MetricParameter<?> p = getParameters().get(name);
		if (p == null) {
			throw new IllegalArgumentException("No such parameter " + getName() + "." + name);
		}
		return p;
	}

	/** @return A builder to create instances of this type */
	Builder build();

	/**
	 * @param initParameters
	 *            The set of parameters to initialize the builder with
	 * @return A builder to create instances of this type, initialized with as many parameters as are common between this parameter set and
	 *         the given map's
	 */
	Builder build(MetricParameterMap initParameters);

	/** An instance of a {@link MetricParameterSet}. Typically a metric, but may be a metric precursor. */
	public interface MetricParameterMap extends Named {
		/** @return The type of this instance */
		MetricParameterSet getType();

		@Override
		default String getName() {
			return getType().getName();
		}

		/** @return The set of parameter values in this instance, by name */
		Map<String, ? extends MetricParameterValue<?>> getParameters();

		/**
		 * @param parameterName
		 *            the name of the parameter to get the value for
		 * @return The value of the parameter in this set with the given name
		 * @throws IllegalArgumentException
		 *             If no parameter with the given name exists in this set
		 */
		default Object getParameter(String parameterName) {
			MetricParameterValue<?> val = getParameters().get(parameterName);
			if (val != null) {
				return val.getValue();
			}
			MetricParameter<?> param = getType().getParameters().get(parameterName);
			if (param == null) {
				throw new IllegalArgumentException("Unrecognized parameter " + getName() + "." + parameterName);
			}
			// Should not have been built if the parameter doesn't have a default
			return param.getDefault().get();
		}
	}
}
