package org.metricity.metric;

import java.util.Collection;

import com.google.common.reflect.TypeToken;

/**
 * Represents a method of accumulating multiple values into a single representative value
 * 
 * @author abutler
 *
 * @param <T>
 *            The type of accumulated value
 * @param <X>
 *            The type of the representative value
 */
public interface MetricAggregation<T, X> {
	/**
	 * Performs the aggregation operation
	 * 
	 * @param values
	 *            The collection of values to accumulate
	 * @return The representative value for the given value collection
	 */
	X aggregate(Collection<? extends T> values);

	/** @return The type of the representative value */
	TypeToken<X> getType();

	/** @return The notional cost of this aggregation's operation */
	double getCost();
}
