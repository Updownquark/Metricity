package org.metricity.metric;

/**
 * An instance (value) of a {@link MetricParameter}
 * 
 * @param <T> The type of the parameter
 */
public interface MetricParameterValue<T> {
	/** @return The parameter that this is a value of */
	MetricParameter<T> getParameter();

	/** @return The parameter value */
	T getValue();

	/** @return The index of this parameter value in its parameter's {@link MetricParameter#getAllowedValues() allowed} or enum values */
	int getEnumIndex();
}