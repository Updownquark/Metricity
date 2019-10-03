package org.metricity.metric;

import java.util.Collection;

import org.metricity.anchor.Anchor;
import org.metricity.anchor.AnchorType;
import org.observe.util.TypeTokens;

import com.google.common.reflect.TypeToken;

/**
 * A metric representing a relationship between one anchor and another
 * 
 * @param <A>
 *            The type of the anchor related to the target anchor
 */
public interface RelationMetricType<A extends Anchor> extends MetricType<A> {
	/** @return The type name of the anchor related to the target anchor */
	AnchorType<A> getTargetAnchorType();

	/**
	 * @param type
	 *            The type to test
	 * @return Whether the type is an anchor type
	 */
	static boolean isRelation(TypeToken<?> type) {
		return Anchor.class.isAssignableFrom(TypeTokens.getRawType(type));
	}

	/**
	 * A {@link SimpleMetricType simple} {@link RelatedMetricType}
	 *
	 * @param <A>
	 *            The type of the anchor related to the target anchor
	 */
	interface Simple<A extends Anchor> extends SimpleMetricType<A>, RelationMetricType<A> {
	}

	/**
	 * Creates a {@link RelatedMetricType} from this relation and a relative metric
	 * 
	 * @param <T>
	 *            The type of the metric
	 * @param relative
	 *            The relative metric
	 * @return The related metric type
	 */
	default <T> RelatedMetricType<T> related(MetricType<T> relative) {
		return MetricTypeBuilder.buildRelated(this, relative);
	}

	/**
	 * Creates a {@link RelatedMetricType} that is also a {@link RelationMetricType} from a relative metric that is also a
	 * {@link RelationMetricType}
	 * 
	 * @param <A2>
	 *            The type of anchor produced by the second relation
	 * @param <MT>
	 *            The type of the produced metric type
	 * @param relative
	 *            The relation relative metric type
	 * @return The relation/related metric type
	 */
	default <A2 extends Anchor, MT extends RelatedMetricType<A2> & RelationMetricType<A2>> MT relatedR(RelationMetricType<A2> relative) {
		return (MT) MetricTypeBuilder.buildRelated(this, relative);
	}

	/**
	 * Creates a {@link RelatedMetricType} that is also a {@link MultiRelationMetricType} from a relative metric that is also a
	 * {@link MultiRelationMetricType}
	 * 
	 * @param <A2>
	 *            The type of anchor produced by the second relation
	 * @param <C>
	 *            The type of anchor collection produced by the second relation
	 * @param <MT>
	 *            The type of the produced metric type
	 * @param relative
	 *            The multi-relation relative metric type
	 * @return The multi-relation/related metric type
	 */
	default <A2 extends Anchor, C extends Collection<A2>, MT extends RelatedMetricType<A2> & RelationMetricType<A2>> MT relatedMR(
			MultiRelationMetricType<A2, C> relative) {
		return (MT) MetricTypeBuilder.buildRelated(this, relative);
	}
}
