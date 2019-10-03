package org.metricity.metric.util;

import org.metricity.metric.service.MetricChannel;

/**
 * A MetricChannel with a time-independent value that it can access. The value may change in real time as a result of user action or any
 * other kind of event, but it does not change with simulation time.
 *
 * @param <T>
 *            The type of value for the channel
 */
public interface SingleValuedChannel<T> extends MetricChannel<T> {
	/**
	 * @return The current value of this channel. The value is simulation-time-independent, but may change as a result of user action or
	 *         other events
	 */
	T getCurrentValue();
}
