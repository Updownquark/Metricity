package org.metricity.metric;

import com.google.common.reflect.TypeToken;

import org.metricity.metric.service.MetricChannelService;

/**
 * A type of information that can be returned to describe some quality of an anchor
 * 
 * @author abutler
 *
 * @param <T>
 *            The type of the metric
 */
public interface MetricType<T> extends MetricParameterSet {
	/**
	 * Builds a {@link Metric}
	 * 
	 * @author abutler
	 *
	 * @param <T>
	 *            The type of the metric
	 */
	interface Builder<T> extends MetricParameterSet.Builder {
		@Override
		MetricType<T> getType();

		@Override
		Builder<T> with(String parameterName, Object value) throws IllegalArgumentException;

		@Override
		Metric<T> build();
	}

	/** @return The type of values the metric represents */
	TypeToken<T> getType();

	/** @return The group name for the metric */
	String getGroupName();

	/** @return The display name for the metric */
	String getDisplayName();

	/** @return Determines whether this metric type is advertised by {@link MetricChannelService#getSupport() metric support} */
	boolean isInternalOnly();

	/** @return Whether This metric type should be advertised to the user */
	boolean isUI();

	@Override
	Builder<T> build();

	@Override
	Builder<T> build(MetricParameterMap initParameters);
}
