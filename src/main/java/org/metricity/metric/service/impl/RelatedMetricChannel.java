package org.metricity.metric.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import org.metricity.anchor.Anchor;
import org.metricity.metric.RelatedMetric;
import org.metricity.metric.service.MetricChannel;
import org.metricity.metric.service.MetricChannelService;
import org.metricity.metric.service.MetricQueryOptions;
import org.metricity.metric.service.MetricQueryResult;
import org.metricity.metric.service.MetricQueryService;
import org.metricity.metric.service.MetricTag;
import org.metricity.metric.util.MetricTimelineWrapper;
import org.metricity.metric.util.SingleValuedChannel;
import org.metricity.metric.util.SmartMetricChannel;
import org.metricity.metric.util.derived.ChannelTemplate;
import org.metricity.metric.util.derived.DynamicDependencyResult;

public class RelatedMetricChannel<T> implements SmartMetricChannel<T> {
	private final Anchor theAnchor;
	private final MetricChannel<? extends Anchor> theRelationChannel;
	/** This channel is only ever non-null if the relation channel is discovered to be immutable and timeless */
	private MetricChannel<? extends T> theRelativeChannel;
	private final RelatedMetric<T> theRelatedMetric;
	private final int hashCode;

	private RelatedMetricChannel(Anchor anchor, MetricChannel<? extends Anchor> relationChannel, RelatedMetric<T> relatedMetric) {
		theAnchor = anchor;
		theRelationChannel = relationChannel;
		theRelatedMetric = relatedMetric;

		hashCode = Objects.hash(theRelationChannel, theRelatedMetric);
	}

	@Override
	public Anchor getAnchor() {
		return theAnchor;
	}

	@Override
	public RelatedMetric<T> getMetric() {
		return theRelatedMetric;
	}

	public MetricChannel<? extends Anchor> getRelationChannel() {
		return theRelationChannel;
	}

	/**
	 * If this channel's {@link #getRelationChannel() relation channel} is both {@link MetricQueryService#IMMUTABLE immutable} and
	 * {@link MetricQueryService#TIMELESS timeless}, the channel for the actual relation values can be discovered and cached.
	 * 
	 * @param depends The channels service to get the channel from
	 * @return The relative channels for the relation value, or null if this is not static
	 */
	public MetricChannel<? extends T> getStaticRelativeChannelIfPossible(MetricChannelService depends) {
		if (theRelativeChannel == null) {
			if (depends.isConstant(theRelationChannel)) {
				MetricQueryResult<? extends Anchor> anchorRes = depends.query(theRelationChannel, null);
				anchorRes.waitFor(1000);
				Anchor anchor = anchorRes.getValue(MetricQueryOptions.get());
				MetricChannel<? extends T> relativeChannel;
				if (anchor == null) {
					relativeChannel = null;
				} else {
					relativeChannel = depends.getChannel(anchor, theRelatedMetric.getRelativeMetric(), null);
					theRelativeChannel = relativeChannel;
				}
			}
		}
		return theRelativeChannel;
	}

	@Override
	public double getCost(MetricChannelService depends) {
		double anchorCost = depends.getCost(theRelationChannel);
		// Resolution doesn't matter
		MetricQueryResult<? extends Anchor> anchorRes = depends.query(theRelationChannel, null);
		Anchor anchor = anchorRes.getValue(MetricQueryOptions.get());
		if (anchor == null) {
			return anchorCost;
		}
		MetricChannel<T> channel = depends.getChannel(anchor, theRelatedMetric.getRelativeMetric(), null);
		if (channel == null) {
			return anchorCost;
		}
		return anchorCost + depends.getCost(channel);
	}

	@Override
	public Set<MetricTag> getTags(MetricChannelService depends) {
		Anchor anchor;
		if (theRelationChannel instanceof SingleValuedChannel) {
			anchor = ((SingleValuedChannel<? extends Anchor>) theRelationChannel).getCurrentValue();
		} else {
			// Resolution doesn't matter
			MetricQueryResult<? extends Anchor> anchorRes = depends.query(theRelationChannel, null);
			anchor = anchorRes.getValue(MetricQueryOptions.get());
		}
		if (anchor == null) {
			return Collections.emptySet();
		}
		MetricChannel<T> channel = depends.getChannel(anchor, theRelatedMetric, null);
		if (channel == null) {
			return Collections.emptySet();
		}
		return depends.getTags(channel);
	}

	@Override
	public boolean isConstant(MetricChannelService depends) {
		MetricChannel<? extends T> relChannel = getStaticRelativeChannelIfPossible(depends);
		if (relChannel == null) {
			// If the relation cannot be resolved statically, we can't know anything about whether we'll be timeless or mutable
			return false;
		}
		// Don't need to query the relation properties because the static relative channel is only available for timeless|immutable
		return depends.isConstant(relChannel);
	}

	@Override
	public MetricQueryResult<T> query(MetricChannelService depends, Consumer<Object> updateReceiver) {
		if (getStaticRelativeChannelIfPossible(depends) != null) {
			return MetricTimelineWrapper.wrap(this,
				(MetricQueryResult<T>) depends.query(getStaticRelativeChannelIfPossible(depends), updateReceiver), v -> v);
		}
		if (updateReceiver == null) {
			return new RelatedMetricTimeline<>(this, //
				depends.<Anchor> query((MetricChannel<Anchor>) theRelationChannel, null), //
				depends, null);
		}
		class RelatedUpdater implements Consumer<Object> {
			RelatedMetricTimeline<T> timeline;

			@Override
			public void accept(Object update) {
				if (timeline != null) {
					timeline.updateRelation(update);
				}
			}
		}
		RelatedUpdater updater = new RelatedUpdater();
		MetricQueryResult<Anchor> relationRes = depends.<Anchor> query((MetricChannel<Anchor>) theRelationChannel, updater);
		updater.timeline = new RelatedMetricTimeline<>(this, relationRes, depends, updateReceiver);
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
		} else if (!(obj instanceof RelatedMetricChannel)) {
			return false;
		}
		RelatedMetricChannel<?> other = (RelatedMetricChannel<?>) obj;
		return theRelationChannel.equals(other.theRelationChannel) && theRelatedMetric.equals(other.theRelatedMetric);
	}

	@Override
	public String toString() {
		return theAnchor + "." + theRelatedMetric;
	}

	public static <T> RelatedMetricChannel<T> createRelatedChannel(Anchor anchor, MetricChannel<? extends Anchor> relationChannel,
		RelatedMetric<T> relatedMetric) {
		return new RelatedMetricChannel<>(anchor, relationChannel, relatedMetric);
	}

	private static class RelatedMetricTimeline<T> extends DynamicDependencyResult<Anchor, T, T> {
		RelatedMetricTimeline(RelatedMetricChannel<T> channel, MetricQueryResult<Anchor> relationResults, MetricChannelService depends,
			Consumer<Object> updates) {
			super(a -> Arrays.asList(a == null ? null : new ChannelTemplate<>(a, channel.getMetric().getRelativeMetric(), true)), //
				ds -> ds.get(0), channel, relationResults, depends, updates);
		}

		@Override
		protected void updateRelation(Object update) {
			super.updateRelation(update);
		}
	}
}
