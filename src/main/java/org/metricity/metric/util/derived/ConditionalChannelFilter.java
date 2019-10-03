package org.metricity.metric.util.derived;

import java.util.function.Function;

import org.metricity.anchor.Anchor;

/**
 * A dynamic channel filter that filters channels based on dependent metric values
 * 
 * @author abutler
 *
 * @param <A>
 *            The type of the anchor
 * @param <T>
 *            The type of the target metric
 * @param <X>
 *            The type of the dependent metric
 */
public interface ConditionalChannelFilter<A extends Anchor, T, X> {
	/**
	 * Produces a channel filter from a {@link DependencyInstanceSet}, which has resolved the actual target anchor/metric
	 * 
	 * @param instances
	 *            The dependency channel data
	 * @return The channel filter
	 */
	ChannelFilter<A, X> filterFor(DependencyInstanceSet<A, T> instances);

	/**
	 * Produces a channel filter from a {@link DependencyValueSet}, which has resolved dependency values
	 * 
	 * @param values
	 *            The dependency value data
	 * @return The channel filter
	 */
	default ChannelFilter<A, X> filterForValues(DependencyValueSet<A, T> values) {
		return filterFor(values);
	}

	/**
	 * Creates a {@link ConditionalChannelFilter} for dependency values
	 * 
	 * @param <A>
	 *            The type of the anchor
	 * @param <T>
	 *            The type of the target metric
	 * @param <X>
	 *            The type of the dependent metric
	 * @param forValues
	 *            The channel filter producer
	 * @return The conditional channel filter
	 */
	public static <A extends Anchor, T, X> ConditionalChannelFilter<A, T, X> forValues(
			Function<DependencyValueSet<A, T>, ChannelFilter<A, X>> forValues) {
		return new ConditionalChannelFilter<A, T, X>() {
			@Override
			public ChannelFilter<A, X> filterFor(DependencyInstanceSet<A, T> instances) {
				return ChannelFilter.first();
			}

			@Override
			public ChannelFilter<A, X> filterForValues(DependencyValueSet<A, T> values) {
				return forValues.apply(values);
			}
		};
	}
}
