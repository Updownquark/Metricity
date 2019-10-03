package org.metricity.metric;

import java.util.Set;

import org.metricity.anchor.AnchorSource;
import org.metricity.anchor.AnchorType;
import org.metricity.metric.service.MetricChannelService;
import org.qommons.BiTuple;

/**
 * A structure detailing the metrics that are supported by a {@link MetricChannelService}
 * 
 * @author abutler
 */
public interface MetricSupport {
	/**
	 * @param anchorSource
	 *            The anchor source to query support for
	 * @param anchorType
	 *            The anchor type to query support for
	 * @return The metric support set of all metrics supported for the given anchor source/type
	 */
	MetricSet<SimpleMetricType<?>> get(AnchorSource anchorSource, AnchorType<?> anchorType);

	/**
	 * @param anchorSource
	 *            The anchor source to query support for
	 * @param anchorType
	 *            The anchor type to query support for
	 * @return The metric support set of all <b>relation</b> metrics supported for the given anchor source/type
	 */
	MetricSet<RelationMetricType.Simple<?>> getRelational(AnchorSource anchorSource, AnchorType<?> anchorType);

	/**
	 * @param anchorSource
	 *            The anchor source to query support for
	 * @param anchorType
	 *            The anchor type to query support for
	 * @return The metric support set of all <b>multi-relation</b> metrics supported for the given anchor source/type
	 */
	MetricSet<MultiRelationMetricType.Simple<?, ?>> getMultiRelational(AnchorSource anchorSource, AnchorType<?> anchorType);

	/**
	 * @param anchorSource
	 *            The anchor source to query support for
	 * @param anchorType
	 *            The anchor type to query support for
	 * @return The set of all <b>non-aggregate</b> metrics supported for the given anchor source/type
	 */
	Set<MetricType<?>> getAllNonAggregate(AnchorSource anchorSource, AnchorType<?> anchorType);

	/**
	 * @param anchorSource
	 *            The anchor source to query support for
	 * @param anchorType
	 *            The anchor type to query support for
	 * @return The set of relation/relative metric tuples representing all <b>aggregate</b> metrics supported for the given anchor
	 *         source/type
	 */
	Set<BiTuple<MultiRelationMetricType<?, ?>, SimpleMetricType<?>>> getAllAggregate(AnchorSource anchorSource, AnchorType<?> anchorType);

	/**
	 * @param anchorSource
	 *            The anchor source to query support for
	 * @param anchorType
	 *            The anchor type to query support for
	 * @param metric
	 *            The metric type to query support for
	 * @return The reason the given metric is not supported for the given anchor source/type, or null if it is supported
	 */
	default String isSupported(AnchorSource anchorSource, AnchorType<?> anchorType, MetricType<?> metric) {
		String msg;
		if (metric instanceof RelatedMetricType) {
			RelatedMetricType<?> related = (RelatedMetricType<?>) metric;
			msg = isSupported(anchorSource, anchorType, related.getRelationMetricType());
			if (msg != null) {
				return msg;
			}
			msg = isSupported(anchorSource, related.getRelationMetricType().getTargetAnchorType(), related.getRelativeMetricType());
			return msg;
		} else if (metric instanceof AggregateMetricType) {
			AggregateMetricType<?, ?> related = (AggregateMetricType<?, ?>) metric;
			msg = isSupported(anchorSource, anchorType, related.getRelationMetricType());
			if (msg != null) {
				return msg;
			}
			msg = isSupported(anchorSource, related.getRelationMetricType().getTargetAnchorType(), related.getRelativeMetricType());
			return msg;
		} else if (metric instanceof SimpleMetricType) {
			if (get(anchorSource, anchorType).isSupported((SimpleMetricType<?>) metric)) {
				return null;
			} else {
				return "Metric " + metric + " is not supported for " + anchorSource + "/" + anchorType;
			}
		} else {
			throw new IllegalArgumentException("Unrecognized metric class " + metric.getClass().getName());
		}
	}
}
