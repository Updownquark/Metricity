package org.metricity.metric.util.derived;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.metricity.anchor.Anchor;
import org.metricity.metric.service.MetricChannel;
import org.metricity.metric.service.MetricQueryService;
import org.metricity.metric.service.MetricTag;
import org.metricity.metric.util.MetricTagImpl;
import org.qommons.collect.BetterCollections;
import org.qommons.collect.BetterHashSet;
import org.qommons.collect.BetterSet;

/**
 * <p>
 * Allows a metric channel to be selected from all those available instead of just the highest-priority one.
 * </p>
 * <p>
 * Another thing I had a vision for but didn't end up using at all. It is implemented though.
 * </p>
 * 
 * @author abutler
 *
 * @param <A>
 *            The type of the anchor
 * @param <T>
 *            The type of the metric dependency
 */
public abstract class ChannelFilter<A extends Anchor, T> {
	/**
	 * Selects a channel to use from the set of all those available from the service
	 * 
	 * @param <X>
	 *            The type of the metric
	 * @param available
	 *            The list of channels available from the service
	 * @param source
	 *            The source of the channels
	 * @return The channel to use, selected from the list. Or null if a satisfactory channel is not found.
	 */
	public abstract <X extends T> MetricChannel<X> selectFrom(List<? extends MetricChannel<X>> available, MetricQueryService source);

	@Override
	public abstract boolean equals(Object o);

	/**
	 * Creates a channel that selects the first channel in the list with a given tag
	 * 
	 * @param <A>
	 *            The type of the anchor
	 * @param <T>
	 *            The type of the dependency
	 * @param category
	 *            The label category
	 * @param label
	 *            The label value
	 * @param useFirstIfNoMatch
	 *            Whether to return the highest-priority channel if no matching channel is found (instead of null)
	 * @return The channel filter
	 */
	public static <A extends Anchor, T> ChannelFilter<A, T> byTag(String category, String label, boolean useFirstIfNoMatch) {
		return byTags(true, useFirstIfNoMatch, category, label);
	}

	/**
	 * Creates a channel that selects the first channel in the list with the given set of tags
	 * 
	 * @param <A>
	 *            The type of the anchor
	 * @param <T>
	 *            The type of the dependency
	 * @param requireAll
	 *            Whether the tag set is to be exclusive (a channel must have all the specified tags to match) or inclusive (a channel may
	 *            have any of the specified tags to match)
	 * @param categoryLabels
	 *            An even-sized set of category/labels corresponding to the set of tags to select on
	 * @param useFirstIfNoMatch
	 *            Whether to return the highest-priority channel if no matching channel is found (instead of null)
	 * @return The channel filter
	 */
	public static <A extends Anchor, T> ChannelFilter<A, T> byTags(boolean requireAll, boolean useFirstIfNoMatch,
			String... categoryLabels) {
		if(categoryLabels.length%2!=0) {
			throw new IllegalArgumentException("Use category1, label1, category2, label2...");
		}
		BetterSet<MetricTag> tags=BetterHashSet.build().withInitialCapacity((int) Math.ceil(categoryLabels.length*3/4)).unsafe().buildSet();
		for(int i=0;i<categoryLabels.length;i+=2){
			tags.add(new MetricTagImpl(categoryLabels[i], categoryLabels[i+1]));
		}
		return new ChannelFilterByTags<>(tags, requireAll, useFirstIfNoMatch);
	}

	/**
	 * Makes this basically equivalent to no filter. Selects the highest-priority channel from the list.
	 * 
	 * @param <A>
	 *            The type of the anchor
	 * @param <T>
	 *            The type of the dependency
	 * @return The channel filter
	 */
	public static <A extends Anchor, T> First<A, T> first() {
		return (First<A, T>) First.instance;
	}

	static class First<A extends Anchor, T> extends ChannelFilter<A, T> {
		static final First<Anchor, Object> instance = new First<>();

		@Override
		public <X extends T> MetricChannel<X> selectFrom(List<? extends MetricChannel<X>> available, MetricQueryService source) {
			return available.isEmpty() ? null : available.get(0);
		}

		@Override
		public boolean equals(Object o) {
			return o instanceof First;
		}

		@Override
		public int hashCode() {
			return 0;
		}

		@Override
		public String toString() {
			return "First";
		}
	}

	static abstract class AbstractChannelFilter<A extends Anchor, T> extends ChannelFilter<A, T> {
		private final List<Object> theComponents;

		public AbstractChannelFilter(List<Object> components) {
			theComponents = new ArrayList<>(components.size());
			theComponents.addAll(components);
		}

		public AbstractChannelFilter(Object... components){
			this(Arrays.asList(components));
		}

		@Override
		public int hashCode() {
			return theComponents.hashCode();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			} else if (o == null || !(getClass().equals(o.getClass()))) {
				return false;
			}
			return theComponents.equals(((AbstractChannelFilter<?, ?>) o).theComponents);
		}

		@Override
		public String toString() {
			StringBuilder str = new StringBuilder(getClass().getSimpleName());
			if (!theComponents.isEmpty()) {
				str.append(": ").append(theComponents);
			}
			return str.toString();
		}
	}

	static abstract class PredicateChannelFilter<A extends Anchor, T> extends AbstractChannelFilter<A, T> {
		private final boolean useFirstIfNoneMatch;

		public PredicateChannelFilter(boolean useFirstIfNoneMatch, List<Object> components) {
			super(components);
			this.useFirstIfNoneMatch = useFirstIfNoneMatch;
		}

		public PredicateChannelFilter(boolean useFirstIfNoneMatch, Object... components) {
			super(components);
			this.useFirstIfNoneMatch = useFirstIfNoneMatch;
		}

		@Override
		public int hashCode() {
			return super.hashCode() + (useFirstIfNoneMatch ? 1 : 0);
		}

		@Override
		public boolean equals(Object o) {
			if (!super.equals(o)) {
				return false;
			}
			return useFirstIfNoneMatch == ((PredicateChannelFilter<?, ?>) o).useFirstIfNoneMatch;
		}

		public abstract boolean test(MetricChannel<? extends T> channel, MetricQueryService source);

		@Override
		public <X extends T> MetricChannel<X> selectFrom(List<? extends MetricChannel<X>> available, MetricQueryService source) {
			if (available.isEmpty()) {
				return null;
			}
			for (MetricChannel<X> channel : available) {
				if (test(channel, source)) {
					return channel;
				}
			}
			return available.get(0);
		}
	}

	static class ChannelFilterByTags<A extends Anchor, T> extends PredicateChannelFilter<A, T> {
		private final BetterSet<MetricTag> theTags;
		private final boolean allRequired;

		protected ChannelFilterByTags(BetterSet<MetricTag> tags, boolean allRequired, boolean useFirstIfNoneMatch) {
			super(useFirstIfNoneMatch, tags, allRequired);
			theTags = BetterCollections.unmodifiableSet(tags);
			this.allRequired = allRequired;
		}

		@Override
		public boolean test(MetricChannel<? extends T> channel, MetricQueryService source) {
			Set<MetricTag> tags = source.getTags(channel);
			if (allRequired) {
				return tags.containsAll(theTags);
			} else {
				return theTags.containsAny(tags);
			}
		}
	}
}
