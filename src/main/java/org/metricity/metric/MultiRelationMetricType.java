package org.metricity.metric;

import java.util.Collection;

import org.metricity.anchor.Anchor;
import org.metricity.anchor.AnchorType;
import org.observe.util.TypeTokens;

import com.google.common.reflect.TypeToken;

/**
 * A metric type that represents a relationship between one anchor and a collection of other anchors (e.g. network members)
 * 
 * @param <A>
 *            The type of the related anchors
 * @param <C>
 *            The type of the anchor collection
 */
public interface MultiRelationMetricType<A extends Anchor, C extends Collection<? extends A>> extends MetricType<C> {
	/** @return The type name of the related anchors */
	AnchorType<A> getTargetAnchorType();

	/**
	 * @param type
	 *            The type to test
	 * @return Whether the given type is a collection of anchors
	 */
	static boolean isMultiRelation(TypeToken<?> type) {
		if (!Collection.class.isAssignableFrom(TypeTokens.getRawType(type))) {
			return false;
		}
		TypeToken<?> component = type.resolveType(Collection.class.getTypeParameters()[0]);
		return component != null && Anchor.class.isAssignableFrom(TypeTokens.getRawType(component));
	}

	/**
	 * A {@link SimpleMetricType simple} {@link MultiRelationMetricType}
	 *
	 * @param <A>
	 *            The type of the related anchors
	 * @param <C>
	 *            The type of the anchor collection
	 */
	interface Simple<A extends Anchor, C extends Collection<? extends A>> extends SimpleMetricType<C>, MultiRelationMetricType<A, C> {
	}

	/**
	 * Creates a {@link StandardAggregateFunctions#COLLECT collection} aggregate metric from this relation and the given relative metric
	 * 
	 * @param <T>
	 *            The type of the metric
	 * @param relative
	 *            The relative metric
	 * @return The aggregate metric
	 */
	default <T> AggregateMetricType<T, Collection<T>> aggregate(MetricType<T> relative) {
		return MetricTypeBuilder.buildAggregate(this, relative, StandardAggregateFunctions.collect(relative.getType()));
	}

	/**
	 * Creates an aggregate metric from this relation, the given relative metric, and the given aggregation
	 * 
	 * @param <T>
	 *            The type of the relative metric
	 * @param <X>
	 *            The type of the aggregate metric
	 * @param relative
	 *            The relative metric
	 * @param aggregation
	 *            The aggregation
	 * @return The aggregate metric
	 */
	default <T, X> AggregateMetricType<T, X> aggregate(MetricType<T> relative, MetricAggregation<? super T, X> aggregation) {
		return MetricTypeBuilder.buildAggregate(this, relative, aggregation);
	}
}
