package org.metricity.metric.service.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.metricity.anchor.Anchor;
import org.metricity.metric.AggregateMetric;
import org.metricity.metric.service.MetricChannel;
import org.metricity.metric.service.MetricChannelService;
import org.metricity.metric.service.MetricQueryOptions;
import org.metricity.metric.service.MetricQueryResult;
import org.metricity.metric.service.MetricQueryService;
import org.metricity.metric.service.MetricResult;
import org.metricity.metric.service.MetricTag;
import org.metricity.metric.util.SingleValuedChannel;
import org.metricity.metric.util.SmartMetricChannel;
import org.metricity.metric.util.derived.ChannelTemplate;
import org.metricity.metric.util.derived.CombinedMetricResults;
import org.metricity.metric.util.derived.DynamicDependencyResult;
import org.qommons.QommonsUtils;

public class AggregateMetricChannel<T, X> implements SmartMetricChannel<X> {
	/** Aggregate queries have to do a bit of book-keeping that costs a bit more than the component queries by themselves */
	private static final double AGGREGATION_COST = 1E-8;

	private final Anchor theAnchor;
	private final MetricChannel<? extends Collection<? extends Anchor>> theRelationChannel;
	private final AggregateMetric<T, X> theAggregateMetric;

	/** This data is only ever non-null if the relation channel is discovered to be immutable and timeless */
	private Collection<? extends Anchor> theRelationAnchors;
	private List<MetricChannel<? extends T>> theRelativeChannels;

	private final int hashCode;

	public AggregateMetricChannel(Anchor anchor, MetricChannel<? extends Collection<? extends Anchor>> relationChannel,
		AggregateMetric<T, X> relatedMetric) {
		theAnchor = anchor;
		theRelationChannel = relationChannel;
		theAggregateMetric = relatedMetric;

		hashCode = Objects.hash(theRelationChannel, theAggregateMetric);
	}

	@Override
	public Anchor getAnchor() {
		return theAnchor;
	}

	@Override
	public AggregateMetric<T, X> getMetric() {
		return theAggregateMetric;
	}

	public MetricChannel<? extends Collection<? extends Anchor>> getRelationChannel() {
		return theRelationChannel;
	}

	/**
	 * If this channel's {@link #getRelationChannel() relation channel} is both {@link MetricQueryService#IMMUTABLE immutable} and
	 * {@link MetricQueryService#TIMELESS timeless}, the list of channels for the actual relation values can be discovered and cached.
	 * 
	 * @param depends The channels service to get the channels from
	 * @return The list of relative channels for each relation value, or null if this is not static
	 */
	public List<MetricChannel<? extends T>> getStaticRelativeChannelsIfPossible(MetricChannelService depends) {
		if (theRelativeChannels == null) {
			if (depends.isConstant(theRelationChannel)) {
				MetricQueryResult<? extends Collection<? extends Anchor>> anchorRes = depends.query(theRelationChannel, null);
				anchorRes.waitFor(1000);
				theRelationAnchors = anchorRes.getValue(MetricQueryOptions.get());
				List<MetricChannel<? extends T>> relativeChannels;
				if (theRelationAnchors == null) {
					theRelativeChannels = Collections.emptyList();
				} else {
					relativeChannels = new ArrayList<>(theRelationAnchors.size());
					for (Anchor a : theRelationAnchors) {
						relativeChannels.add(depends.getChannel(a, theAggregateMetric.getRelativeMetric(), null));
					}
					theRelativeChannels = Collections.unmodifiableList(relativeChannels);
				}
			}
		}
		return theRelativeChannels;
	}

	public Collection<? extends Anchor> getStaticRelationAnchors(MetricChannelService depends) {
		getStaticRelativeChannelsIfPossible(depends);
		return theRelationAnchors;
	}

	@Override
	public double getCost(MetricChannelService depends) {
		double anchorCost = depends.getCost(theRelationChannel);
		// Resolution doesn't matter
		MetricQueryResult<? extends Collection<? extends Anchor>> anchorRes = depends.query(theRelationChannel, null);
		Collection<? extends Anchor> anchors = anchorRes.getValue(MetricQueryOptions.get());
		if (anchors == null) {
			return anchorCost;
		}
		double relativeCost = 0;
		for (Anchor anchor : anchors) {
			MetricChannel<T> channel = depends.getChannel(anchor, theAggregateMetric.getRelativeMetric(), null);
			if (channel == null) {
				return anchorCost;
			}
			relativeCost += depends.getCost(channel);
		}
		return anchorCost + relativeCost//
			+ theAggregateMetric.getType().getAggregation().getCost() * anchors.size()// Cost is assumed to be linear
			+ AGGREGATION_COST;
	}

	@Override
	public Set<MetricTag> getTags(MetricChannelService depends) {
		Collection<? extends Anchor> anchors;
		if (theRelationChannel instanceof SingleValuedChannel) {
			anchors = ((SingleValuedChannel<? extends Collection<? extends Anchor>>) theRelationChannel).getCurrentValue();
		} else {
			// We'll return all tags common to all the relative channels for the anchors in the relation at the time
			MetricQueryResult<? extends Collection<? extends Anchor>> anchorRes = depends.query(theRelationChannel, null);
			anchors = anchorRes.getValue(MetricQueryOptions.get());
		}
		if (anchors == null) {
			return Collections.emptySet();
		}
		Set<MetricTag> tags = null;
		for (Anchor anchor : anchors) {
			MetricChannel<T> channel = depends.getChannel(anchor, theAggregateMetric.getRelativeMetric(), null);
			if (channel == null) {
				continue;
			}
			Set<MetricTag> depTags = depends.getTags(channel);
			if (depTags.isEmpty()) {
				return Collections.emptySet();
			} else if (tags == null) {
				tags = new TreeSet<>(depTags);
			} else {
				tags.retainAll(depTags);
				if (tags.isEmpty()) {
					return Collections.emptySet();
				}
			}
		}
		return Collections.unmodifiableSet(tags);
	}

	@Override
	public boolean isConstant(MetricChannelService depends) {
		List<MetricChannel<? extends T>> relChannels = getStaticRelativeChannelsIfPossible(depends);
		if (relChannels == null) {
			// If the relation channel is mutable or time-sensitive, we can't know anything about whether we'll be timeless or mutable
			return false;
		}
		// Don't need to query the relation properties because the static relative channel is only available for constant
		for (MetricChannel<? extends T> relChannel : relChannels) {
			if (relChannel != null) {
				if (!depends.isConstant(relChannel))
					return false;
			}
		}
		return true;
	}

	@Override
	public MetricQueryResult<X> query(MetricChannelService depends, Consumer<Object> updateReceiver) {
		if (getStaticRelationAnchors(depends) != null)
			return new StaticAggregateResult<>(this, updateReceiver, depends);
		else if (updateReceiver == null)
			return new AggregateMetricResult<>(this, //
				depends.<Collection<Anchor>> query((MetricChannel<Collection<Anchor>>) theRelationChannel, null), //
				depends, null);
		class AggregateUpdater implements Consumer<Object> {
			AggregateMetricResult<T, X> timeline;

			@Override
			public void accept(Object update) {
				if (timeline != null) {
					timeline.updateRelation(update);
				}
			}
		}
		AggregateUpdater updater = new AggregateUpdater();
		MetricQueryResult<Collection<Anchor>> relationRes = depends
			.<Collection<Anchor>> query((MetricChannel<Collection<Anchor>>) theRelationChannel, updater);
		updater.timeline = new AggregateMetricResult<>(this, relationRes, depends, updateReceiver);
		return updater.timeline;
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (!(obj instanceof AggregateMetricChannel)) {
			return false;
		}
		AggregateMetricChannel<?, ?> other = (AggregateMetricChannel<?, ?>) obj;
		return theRelationChannel.equals(other.theRelationChannel) && theAggregateMetric.equals(other.theAggregateMetric);
	}

	@Override
	public String toString() {
		return theAnchor + "." + theAggregateMetric;
	}

	public static <T, X> AggregateMetricChannel<T, X> createAggregateChannel(Anchor anchor,
		MetricChannel<? extends Collection<? extends Anchor>> relationChannel, AggregateMetric<T, X> aggregateMetric) {
		return new AggregateMetricChannel<>(anchor, relationChannel, aggregateMetric);
	}

	private static class AggregateMetricResult<T, X> extends DynamicDependencyResult<Collection<Anchor>, T, X> {
		AggregateMetricResult(AggregateMetricChannel<T, X> channel, MetricQueryResult<Collection<Anchor>> relationResults,
			MetricChannelService depends, Consumer<Object> updates) {
			super(c -> {
				return QommonsUtils.filterMap(c, a -> a != null,
					a -> new ChannelTemplate<>(a, channel.getMetric().getRelativeMetric(), true));
			}, ds -> channel.getMetric().getType().getAggregation().aggregate(ds.values()), channel, relationResults, depends, updates);
		}

		@Override
		protected void updateRelation(Object update) {
			super.updateRelation(update);
		}
	}

	private static class StaticAggregateResult<T, X> extends CombinedMetricResults<X> {
		private final Consumer<Object> theUpdates;
		private final List<Anchor> theAnchors;
		private final List<MetricQueryResult<? extends T>> theResults;

		StaticAggregateResult(AggregateMetricChannel<T, X> channel, Consumer<Object> updates, MetricChannelService depends) {
			super(channel, channel.getStaticRelativeChannelsIfPossible(depends).size());
			theUpdates = updates;
			Collection<? extends Anchor> anchors = channel.getStaticRelationAnchors(depends);
			if (anchors instanceof List) {
				theAnchors = (List<Anchor>) anchors;
			} else if (anchors == null) {
				theAnchors = Collections.emptyList();
			} else {
				theAnchors = new ArrayList<>(anchors.size());
				theAnchors.addAll(anchors);
			}
			theResults = new ArrayList<>(theAnchors.size());
			for (int i = 0; i < theAnchors.size(); i++) {
				int dependency = i;
				theResults.add(depends.query(channel.getStaticRelativeChannelsIfPossible(depends).get(i),
					theUpdates == null ? null : u -> _update(dependency, u)));
			}
		}

		@Override
		public AggregateMetricChannel<T, X> getChannel() {
			return (AggregateMetricChannel<T, X>) super.getChannel();
		}

		@Override
		protected boolean isRequired(int dependency) {
			return theAnchors.get(dependency) != null;
		}

		@Override
		protected MetricQueryResult<?> getDependency(int dependency) {
			return theResults.get(dependency);
		}

		@Override
		public MetricResult<X> getResult(MetricResult<X> result, MetricQueryOptions options) {
			List<T> values = new ArrayList<>(theAnchors.size());
			boolean anyAvailable = false;
			boolean anyComputing = false;
			for (int i = 0; i < theResults.size(); i++) {
				MetricQueryResult<? extends T> timeline = theResults.get(i);
				if (timeline == null) {
					if (theAnchors.get(i) == null) {
						values.add(null);
					} else {
						return result.unavailable(false);
					}
				} else {
					((MetricQueryResult<X>) timeline).getResult(result, options);
					if (!result.isValid()) {
						return result;
					} else if (result.isAvailable()) {
						anyAvailable = true;
					} else {
						anyComputing |= result.isComputing();
					}
					values.add((T) result.get());
				}
			}
			if (!anyAvailable) {
				return result.unavailable(anyComputing);
			}
			result.setValue(getChannel().getMetric().getType().getAggregation().aggregate(values));
			return result;
		}

		void _update(int dependency, Object dependencyUpdate) {
			theUpdates.accept(dependencyUpdate);
		}
	}
}
