package org.metricity.metric;

/**
 * Represents the concept of a specific piece of information about a specific type of anchor (thing)
 * 
 * @author abutler
 *
 * @param <T>
 *            The type of value that the metric represents
 */
public interface Metric<T> extends MetricParameterSet.MetricParameterMap {
	@Override
	MetricType<T> getType();

	/** @return A name to display for this metric */
	default String getDisplayName() {
		StringBuilder str = new StringBuilder(getType().getDisplayName());
		boolean first = true;
		for (MetricParameterValue<?> param : getParameters().values()) {
			if (param != null) {
				if (first) {
					str.append(" (");
				} else {
					str.append(", ");
				}
				first = false;
				str.append(param.getValue() == null ? "None" : param.getValue().toString());
			}
		}
		if (!first) {
			str.append(')');
		}
		return str.toString();
	}
}
