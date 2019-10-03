package org.metricity.metric;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import org.qommons.Named;

import com.google.common.reflect.TypeToken;

/**
 * Some metrics are parameterized, i.e. multiple distinct metric instances may be produced from a single type. A metric parameter is a
 * named, typed structure for which a value must be specified to produce a metric.
 * 
 * @param <P> The type of the parameter value
 */
public interface MetricParameter<P> extends Named {
	/** @return The type of the parameter value */
	TypeToken<P> getType();

	/** @return Whether <code>null</code> is an acceptable value for this parameter */
	boolean isNullable();

	/** @return A set of named filters that a value must pass to be acceptable for this parameter */
	Map<String, Predicate<? super P>> getFilters();

	/**
	 * @return An optional default value for this parameter. If {@link Optional#isPresent() present}, this parameter need not be specified
	 *         to create the metric.
	 */
	Optional<P> getDefault();

	/**
	 * @return The exclusive list of values that are acceptable for parameter, or an empty list if this parameter's potential value set is
	 *         infinite or large
	 */
	List<P> getAllowedValues();

	/**
	 * @param paramValue The value to test for this parameter
	 * @return A message to display if the given value is not valid for this parameter, or null if it is valid
	 */
	default String filter(P paramValue) {
		if (paramValue == null && !isNullable()) {
			return "Parameter must have a value";
		} else if (!getAllowedValues().isEmpty() && !getAllowedValues().contains(paramValue)) {
			StringBuilder msg = new StringBuilder("Parameter must be one of ");
			for (int i = 0; i < getAllowedValues().size(); i++) {
				if (getAllowedValues().size() > 2 && i > 0) {
					msg.append(", ");
				}
				if (i > 0 && i == getAllowedValues().size() - 1) {
					if (getAllowedValues().size() == 2) {
						msg.append(' ');
					}
					msg.append("or ");
				}
				msg.append(getAllowedValues().get(i));
			}
			msg.append(", not ").append(paramValue);
			return msg.toString();
		}
		StringBuilder msg = null;
		for (Map.Entry<String, Predicate<? super P>> filter : getFilters().entrySet()) {
			if (!filter.getValue().test(paramValue)) {
				if (msg == null) {
					msg = new StringBuilder();
				} else {
					msg.append(", ");
				}
				msg.append(filter.getKey());
			}
		}
		return msg == null ? null : msg.toString();
	}
}