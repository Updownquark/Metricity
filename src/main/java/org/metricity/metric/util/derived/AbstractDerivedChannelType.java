package org.metricity.metric.util.derived;

import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import org.metricity.anchor.Anchor;
import org.metricity.anchor.AnchorSource;
import org.metricity.anchor.AnchorType;
import org.metricity.metric.Metric;
import org.metricity.metric.SimpleMetric;
import org.metricity.metric.SimpleMetricType;
import org.metricity.metric.service.MetricChannelService;
import org.metricity.metric.service.MetricTag;
import org.metricity.metric.service.TaggedMetricChannel;
import org.metricity.metric.util.MetricSatisfier;
import org.metricity.metric.util.SmartMetricChannel;

public abstract class AbstractDerivedChannelType<T> implements MetricSatisfier<T> {
	private final SimpleMetricType<T> theMetric;
	private final Predicate<AnchorSource> theAnchorSource;
	private final Predicate<AnchorType<?>> theAnchorType;
	private final Function<Anchor, String> theAnchorFilter;
	private final Function<? super SimpleMetric<T>, String> theMetricFilter;
	private final Set<MetricTag> theTags;
	private final double theDerivationCost;

	public AbstractDerivedChannelType(SimpleMetricType<T> metric, Predicate<AnchorSource> anchorSource, Predicate<AnchorType<?>> anchorType,
		Function<Anchor, String> anchorFilter, Function<? super SimpleMetric<T>, String> metricFilter, Set<MetricTag> tags,
		double derivationCost) {
		theMetric = metric;
		theAnchorSource = anchorSource;
		theAnchorType = anchorType;
		theAnchorFilter = anchorFilter;
		theMetricFilter = metricFilter;
		theTags = tags;
		theDerivationCost = derivationCost;
	}

	@Override
	public SimpleMetricType<T> getMetric() {
		return theMetric;
	}

	public Set<MetricTag> getTags() {
		return theTags;
	}

	public double getDerivationCost() {
		return theDerivationCost;
	}

	@Override
	public String applies(AnchorSource anchorSource, AnchorType<?> anchorType) {
		if (!theAnchorSource.test(anchorSource)) {
			return new StringBuilder("Anchor source ").append(anchorSource).append(" does not apply to " + theAnchorSource).toString();
		} else if (!theAnchorType.test(anchorType)) {
			return new StringBuilder("Anchor type ").append(anchorType).append(" does not apply to "+theAnchorType).toString();
		} else {
			return null;
		}
	}

	protected String applies(Anchor anchor, Metric<T> metric) {
		String err = applies(anchor.getSource(), anchor.getType());
		if (err == null && theAnchorFilter != null) {
			err = theAnchorFilter.apply(anchor);
		}
		if (err == null && theMetricFilter != null) {
			err = theMetricFilter.apply((SimpleMetric<T>) metric);
		}
		return err;
	}

	@Override
	public String toString() {
		return theMetric + " (" + theAnchorSource + ", " + theAnchorType + ")";
	}

	public static abstract class AbstractDerivedChannel<T> implements SmartMetricChannel<T>, TaggedMetricChannel<T> {
		private final AbstractDerivedChannelType<T> theType;
		private final Anchor theAnchor;
		private final Metric<T> theMetric;

		private int hashCode;

		public AbstractDerivedChannel(AbstractDerivedChannelType<T> type, Anchor anchor, Metric<T> metric) {
			theType = type;
			theAnchor = anchor;
			theMetric = metric;

			hashCode = -1;
		}

		public AbstractDerivedChannelType<T> getType() {
			return theType;
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
			return theType.getTags();
		}

		@Override
		public Set<MetricTag> getTags(MetricChannelService depends) {
			return getTags();
		}

		@Override
		public int hashCode() {
			if (hashCode == -1) {
				hashCode = genHashCode();
			}
			return hashCode;
		}

		protected int genHashCode() {
			return theAnchor.hashCode() * 17 + theMetric.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			} else if (!(obj instanceof AbstractDerivedChannel)) {
				return false;
			}
			AbstractDerivedChannel<?> other = (AbstractDerivedChannel<?>) obj;
			if (hashCode != -1 && other.hashCode != -1 && hashCode != other.hashCode) {
				return false;
			}
			return theType == other.theType//
					&& theAnchor.equals(other.theAnchor)//
					&& theMetric.equals(other.theMetric);
		}

		@Override
		public String toString() {
			return theAnchor + "." + theMetric;
		}
	}
}
