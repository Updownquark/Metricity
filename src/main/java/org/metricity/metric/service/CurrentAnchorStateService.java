package org.metricity.metric.service;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.metricity.anchor.Anchor;
import org.metricity.anchor.AnchorSelectionService;
import org.metricity.anchor.AnchorType;
import org.metricity.metric.AnchorMetric;
import org.metricity.metric.Metric;
import org.observe.Observable;
import org.observe.Subscription;
import org.observe.collect.CollectionChangeType;
import org.observe.collect.ObservableCollection;
import org.observe.collect.ObservableSortedSet;
import org.qommons.Causable;

/**
 * <p>
 * This interface provides information about anchors and metrics that are relevant to the current frame. Most metric views will use this
 * interface as their data source.
 * </p>
 * <p>
 * For consumers that are only interested in data concerning the current time, this class presents anchor and metric information in a way
 * that is far easier to deal with than the raw {@link AnchorSelectionService} and {@link MetricChannelService} interfaces.
 * </p>
 * <p>
 * The produced API structures from this interface are also very easily translatable into UI models.
 * </p>
 */
public interface CurrentAnchorStateService {
	/** Represents the real-time granularity at which a consumer desires to be notified about metric information */
	public enum NotificationLevel {
		/**
		 * Notification events occur for every change, including intermediate changes when an old value is invalid but a new one has not yet
		 * been computed
		 */
		VERBOSE,
		/** Notification events occur for all metrics whose computation is complete */
		COMPLETE_EACH,
		/** Notification events occur only after computation is complete for all metrics for an anchor */
		COMPLETE_ALL
	}

	/**
	 * A view of metric data from the service. This class provides data from a table of data, anchors x metrics. When an anchor is
	 * {@link #watchAnchor(Anchor) watched}, all metrics that have been {@link #watchMetric(Metric) watched} will be supplied for that
	 * anchor and vice-versa and the consumer will be notified of changes to that metric.
	 */
	interface AnchorStateView {
		/**
		 * @param level The leval at which the consumer desires to be notified of metric computation progress
		 * @return This view
		 */
		AnchorStateView withNotifyLevel(NotificationLevel level);

		/**
		 * Informs this view that the consumer is interested in data regarding the given anchor
		 * 
		 * @param anchor The anchor to provide metric information for
		 * @return This view
		 */
		default AnchorStateView watchAnchor(Anchor anchor) {
			return watchAnchors(//
				Arrays.asList(anchor));
		}

		/**
		 * Informs this view that the consumer is interested in data regarding the given anchors
		 * 
		 * @param anchors The anchors to provide metric information for
		 * @return This view
		 */
		AnchorStateView watchAnchors(Collection<? extends Anchor> anchors);

		/**
		 * Informs this view that the consumer is interested in data regarding the given metric
		 * 
		 * @param metric The metric to provide information for each {@link #watchAnchor(Anchor) watched anchor}
		 * @return This view
		 */
		default AnchorStateView watchMetric(Metric<?> metric) {
			return watchMetrics(Arrays.asList(metric));
		}

		/**
		 * Informs this view that the consumer is interested in data regarding the given metrics
		 * 
		 * @param metrics The metrics to provide information for each {@link #watchAnchor(Anchor) watched anchor}
		 * @return This view
		 */
		AnchorStateView watchMetrics(Collection<? extends Metric<?>> metrics);

		/**
		 * Informs this view that the consumer is interested in data regarding the given metrics
		 * 
		 * @param metrics The metrics to provide information for each {@link #watchAnchor(Anchor) watched anchor}
		 * @return This view
		 */
		default AnchorStateView watchMetrics(Metric<?>... metrics) {
			return watchMetrics(Arrays.asList(metrics));
		}

		/**
		 * Informs this view that the consumer is no longer interested in data regarding the given anchor
		 * 
		 * @param anchor The anchor to cease providing metric information for
		 */
		default void unwatchAnchor(Anchor anchor) {
			unwatchAnchors(//
				Arrays.asList(anchor));
		}

		/**
		 * Informs this view that the consumer is no longer interested in data regarding the given anchors
		 * 
		 * @param anchors The anchors to cease providing metric information for
		 */
		void unwatchAnchors(Collection<? extends Anchor> anchors);

		/**
		 * Informs this view that the consumer is no longer interested in data regarding the given metric
		 * 
		 * @param metric The metric to cease providing metric information for
		 */
		default void unwatchMetric(Metric<?> metric) {
			unwatchMetrics(//
				Arrays.asList(metric));
		}

		/**
		 * Informs this view that the consumer is no longer interested in data regarding the given metrics
		 * 
		 * @param metrics The metrics to cease providing metric information for
		 */
		void unwatchMetrics(Collection<? extends Metric<?>> metrics);

		/** Un-watches all anchors and metrics in this view */
		void clear();
	}

	/** An {@link AnchorStateView} that provides its data in a more traditional listener-style way */
	interface SimpleAnchorStateView extends AnchorStateView {
		@Override
		SimpleAnchorStateView withNotifyLevel(NotificationLevel level);

		@Override
		default SimpleAnchorStateView watchAnchor(Anchor anchor) {
			return watchAnchors(//
				Arrays.asList(anchor));
		}

		@Override
		SimpleAnchorStateView watchAnchors(Collection<? extends Anchor> anchors);

		@Override
		default SimpleAnchorStateView watchMetric(Metric<?> metric) {
			return watchMetrics(Arrays.asList(metric));
		}

		@Override
		SimpleAnchorStateView watchMetrics(Collection<? extends Metric<?>> metrics);

		@Override
		default SimpleAnchorStateView watchMetrics(Metric<?>... metrics) {
			return watchMetrics(Arrays.asList(metrics));
		}

		@Override
		default void unwatchAnchor(Anchor anchor) {
			unwatchAnchors(//
				Arrays.asList(anchor));
		}

		@Override
		void unwatchAnchors(Collection<? extends Anchor> anchors);

		@Override
		default void unwatchMetric(Metric<?> metric) {
			unwatchMetrics(//
				Arrays.asList(metric));
		}

		@Override
		void unwatchMetrics(Collection<? extends Metric<?>> metrics);

		@Override
		void clear();

		/**
		 * @param <T> The type of the metric
		 * @param anchor The anchor to get the metric value for
		 * @param metric The metric to get the value for
		 * @return The metric result containing the value or computation state for the given metric regarding the given anchor
		 */
		<T> MetricResult<T> getResult(Anchor anchor, Metric<T> metric);

		/**
		 * @param <T> The type of the metric
		 * @param anchor The anchor to get the metric value for
		 * @param metric The metric to get the value for
		 * @return The value for the given metric regarding the given anchor
		 */
		default <T> T get(Anchor anchor, Metric<T> metric) {
			return getResult(anchor, metric).get();
		}

		/**
		 * @param anchor The anchor to check
		 * @param metric The metric to check
		 * @return Whether the metric for the given anchor is supported at the current time
		 */
		default boolean isSupported(Anchor anchor, Metric<?> metric) {
			return getResult(anchor, metric).isValid();
		}

		/**
		 * @param anchor The anchor to check
		 * @param metric The metric to check
		 * @return Whether the metric for the given anchor has been computed for the current time
		 */
		default boolean isAvailable(Anchor anchor, Metric<?> metric) {
			return getResult(anchor, metric).isAvailable();
		}

		/**
		 * @param anchor The anchor to check
		 * @param metric The metric to check
		 * @return Whether the metric for the given anchor is scheduled for computation or is currently being computed
		 */
		default boolean isComputing(Anchor anchor, Metric<?> metric) {
			return getResult(anchor, metric).isComputing();
		}

		/**
		 * @param onChange The listener to be notified of metric changes to anchors in this view
		 * @return The subscription to unsubscribe to cease notification
		 */
		Subscription onChange(Consumer<ValueStateChangeEvent> onChange);
	}

	/** Provided by {@link SimpleAnchorStateView} when watched metric values change for the watched anchors */
	class ValueStateChangeEvent extends Causable {
		private final Set<Anchor> theAnchors;
		private final Set<Metric<?>> theMetrics;

		public ValueStateChangeEvent(Object cause, Set<Anchor> anchors, Set<Metric<?>> metrics) {
			super(cause);
			theAnchors = anchors;
			theMetrics = metrics;
		}

		/** @return The set of anchors for which any metric was changed */
		public Set<Anchor> getAffectedAnchors() {
			return theAnchors;
		}

		/** @return The set of metrics for which the value changed for any anchor */
		public Set<Metric<?>> getAffectedMetrics() {
			return theMetrics;
		}
	}

	/** A {@link AnchorStateView} that provides its data in an ObServable format. */
	interface AnchorMetricController extends AnchorStateView, Subscription {
		@Override
		AnchorMetricController withNotifyLevel(NotificationLevel level);

		@Override
		default AnchorMetricController watchAnchor(Anchor anchor) {
			return watchAnchors(//
				Arrays.asList(anchor));
		}

		@Override
		AnchorMetricController watchAnchors(Collection<? extends Anchor> anchors);

		@Override
		default AnchorMetricController watchMetric(Metric<?> metric) {
			return watchMetrics(Arrays.asList(metric));
		}

		@Override
		default AnchorMetricController watchMetrics(Metric<?>... metrics) {
			return watchMetrics(Arrays.asList(metrics));
		}

		@Override
		AnchorMetricController watchMetrics(Collection<? extends Metric<?>> metric);

		@Override
		default void unwatchAnchor(Anchor anchor) {
			unwatchAnchors(//
				Arrays.asList(anchor));
		}

		@Override
		void unwatchAnchors(Collection<? extends Anchor> anchors);

		@Override
		default void unwatchMetric(Metric<?> metric) {
			unwatchMetrics(//
				Arrays.asList(metric));
		}

		@Override
		void unwatchMetrics(Collection<? extends Metric<?>> metrics);

		@Override
		void clear();

		/**
		 * @return An {@link ObservableCollection} of {@link AnchorMetric}s, each of which supplies data for all {@link #watchMetric(Metric)
		 *         watched} metrics for a single {@link #watchAnchor(Anchor) watched} anchor. An element is added when an anchor is watched,
		 *         removed when its anchor is {@link #unwatchAnchor(Anchor) unwatched}, and a {@link CollectionChangeType#set set} event is
		 *         fired when any of the anchor's metric values change.
		 */
		ObservableCollection<AnchorMetric> get();

		/**
		 * @param anchor The anchor to get the metrics for
		 * @return The AnchorMetric for the given anchor
		 */
		AnchorMetric get(Anchor anchor);
	}

	/**
	 * Allows sorting of anchors in the AnchorMetric {@link AnchorMetricController#get() collection} by metric value
	 *
	 * @param <M> The type of the metric
	 */
	public class AnchorSorter<M> {
		/** The metric to sort by */
		public final Metric<M> sortMetric;
		/** The sorter for the metric value */
		public final Comparator<? super M> sorter;

		/**
		 * @param sortMetric The metric to sort by
		 * @param sorter The sorter for the metric value
		 */
		public AnchorSorter(Metric<M> sortMetric, Comparator<? super M> sorter) {
			this.sortMetric = sortMetric;
			this.sorter = sorter;
		}
	}

	/** @return All anchors that exist in the {@link AnchorSelectionService#getSelectedWorld() selected world} at the current time */
	ObservableSortedSet<Anchor> getCurrentAnchors();

	/**
	 * @param type The type name of the anchors to get
	 * @return All anchors of the given type that exist in the {@link AnchorSelectionService#getSelectedWorld() selected world} at the
	 *         current time
	 */
	<A extends Anchor> ObservableSortedSet<A> getCurrentAnchors(AnchorType<A> type);

	/** @return A simple view of anchor-metric data backed by this service */
	SimpleAnchorStateView createState();

	/**
	 * Creates an observable view of anchor-metric data backed by this service
	 * 
	 * @param until The observable to cause the unsubscription of the new view
	 * @return The anchor-metric view
	 */
	default AnchorMetricController createAnchorMetrics(Observable<?> until) {
		return createAnchorMetrics(Collections.emptyList(), until);
	}

	/**
	 * Creates an observable view of anchor-metric data backed by this service
	 * 
	 * @param sorting Sorting information to order the AnchorMetric collection
	 * @param until The observable to cause the unsubscription of the new view
	 * @return The anchor-metric view
	 */
	AnchorMetricController createAnchorMetrics(List<AnchorSorter<?>> sorting, Observable<?> until);

	/** @return An interface providing information on the current state of metric computation by this service */
	MetricWork getWork();
}
