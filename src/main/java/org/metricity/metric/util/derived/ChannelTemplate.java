package org.metricity.metric.util.derived;

import java.util.Objects;

import org.metricity.anchor.Anchor;
import org.metricity.metric.Metric;

/**
 * A notional channel for a metric dependency
 * 
 * @param <T> The type of the dependency
 */
public class ChannelTemplate<T> {
	/** The anchor to get metric values for */
	public final Anchor anchor;
	/** The metric to get values for */
	public final Metric<? extends T> metric;
	/** Whether this dependency is required to produce derived values */
	public final boolean required;
	/** A channel filter to use instead of merely getting the highest-priority channel */
	public final ChannelFilter<Anchor, T> channelFilter;
	private int hashCode;

	/**
	 * @param anchor The anchor to get metric values for
	 * @param metric The metric to get values for
	 * @param required Whether this dependency is required to produce derived values
	 */
	public ChannelTemplate(Anchor anchor, Metric<? extends T> metric, boolean required) {
		this(anchor, metric, required, null);
	}

	/**
	 * @param anchor The anchor to get metric values for
	 * @param metric The metric to get values for
	 * @param required Whether this dependency is required to produce derived values
	 * @param channelFilter A channel filter to use instead of merely getting the highest-priority channel
	 */
	public ChannelTemplate(Anchor anchor, Metric<? extends T> metric, boolean required, ChannelFilter<Anchor, T> channelFilter) {
		this.anchor = anchor;
		this.metric = metric;
		this.required = required;
		this.channelFilter = channelFilter;
		hashCode = -1;
	}

	@Override
	public int hashCode() {
		if (hashCode == -1) {
			hashCode = anchor.hashCode() + metric.hashCode() + (channelFilter == null ? 0 : channelFilter.hashCode());
		}
		return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (!(obj instanceof ChannelTemplate)) {
			return false;
		}
		ChannelTemplate<?> other = (ChannelTemplate<?>) obj;
		if (hashCode != -1 && other.hashCode != -1 && hashCode != other.hashCode) {
			return false;
		}
		return anchor.equals(other.anchor) && metric.equals(other.metric) && Objects.equals(channelFilter, other.channelFilter);
	}

	@Override
	public String toString() {
		return anchor + "." + metric;
	}
}
