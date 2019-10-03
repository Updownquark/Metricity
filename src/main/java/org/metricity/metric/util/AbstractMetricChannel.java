package org.metricity.metric.util;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.metricity.anchor.Anchor;
import org.metricity.metric.Metric;
import org.metricity.metric.service.MetricQueryService;
import org.metricity.metric.service.MetricTag;
import org.metricity.metric.service.TaggedMetricChannel;

public class AbstractMetricChannel<T> implements TaggedMetricChannel<T> {
	private final MetricQueryService theSource;
	private final Anchor theAnchor;
	private final Metric<T> theMetric;
	private final Set<MetricTag> theTags;
	private final int hashCode;

	public AbstractMetricChannel(MetricQueryService source, Anchor anchor, Metric<T> metric, Set<MetricTag> tags) {
		theSource = source;
		theAnchor = anchor;
		theMetric = metric;
		theTags = (tags == null || tags.isEmpty()) ? Collections.emptySet() : Collections.unmodifiableSet(new TreeSet<>(tags));
		hashCode = Objects.hash(theSource, theAnchor, theMetric);
	}

	protected MetricQueryService getSource() {
		return theSource;
	}

	@Override
	public Anchor getAnchor() {
		return theAnchor;
	}

	@Override
	public Metric<T> getMetric() {
		return theMetric;
	}

	@Override
	public Set<MetricTag> getTags() {
		return theTags;
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (obj == null || getClass() != obj.getClass() || hashCode != obj.hashCode()) {
			return false;
		}
		AbstractMetricChannel<?> other = (AbstractMetricChannel<?>) obj;
		return theSource.equals(other.theSource) && theAnchor.equals(other.theAnchor) && theMetric.equals(other.theMetric);
	}

	@Override
	public String toString() {
		return theAnchor + "." + theMetric;
	}
}
