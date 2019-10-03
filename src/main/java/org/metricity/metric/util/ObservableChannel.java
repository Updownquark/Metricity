package org.metricity.metric.util;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.metricity.anchor.Anchor;
import org.metricity.metric.Metric;
import org.metricity.metric.service.MetricChannelService;
import org.metricity.metric.service.MetricQueryOptions;
import org.metricity.metric.service.MetricQueryResult;
import org.metricity.metric.service.MetricResult;
import org.metricity.metric.service.MetricTag;
import org.observe.ObservableValue;
import org.observe.Subscription;
import org.qommons.Transaction;

/**
 * A metric channel defined using {@link HelperMetricServiceComponent.ForMetric#statically(java.util.function.BiFunction)}
 * 
 * @param <T> The type of the metric
 */
public class ObservableChannel<T> implements SmartMetricChannel<T>, SingleValuedChannel<T> {
	private final Anchor theAnchor;
	private final Metric<T> theMetric;
	private final ObservableValue<? extends T> theValue;
	private final Set<MetricTag> theTags;
	private final int hashCode;

	ObservableChannel(Anchor anchor, Metric<T> metric, ObservableValue<? extends T> value, Set<MetricTag> tags) {
		theAnchor = anchor;
		theMetric = metric;
		theValue = value;
		theTags = (tags == null || tags.isEmpty()) ? Collections.emptySet() : Collections.unmodifiableSet(new TreeSet<>(tags));
		hashCode = Objects.hash(theAnchor, theMetric, theValue);
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
	public double getCost(MetricChannelService depends) {
		return 1E-9; // No way of knowing this from the ObSeve API
	}

	@Override
	public Set<MetricTag> getTags(MetricChannelService depends) {
		return theTags;
	}

	@Override
	public MetricQueryResult<T> query(MetricChannelService depends, Consumer<Object> initialListener) {
		ObservableResults<T> res = new ObservableResults<>(this);
		if (initialListener != null)
			res.subscription = res.notifyOnChange(initialListener);
		return res;
	}

	@Override
	public boolean isConstant(MetricChannelService depends) {
		return false;
	}

	@Override
	public T getCurrentValue() {
		return theValue.get();
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (!(obj instanceof ObservableChannel)) {
			return false;
		}
		ObservableChannel<?> other = (ObservableChannel<?>) obj;
		return theAnchor.equals(other.theAnchor)//
			&& theMetric.equals(other.theMetric)//
			&& theValue.equals(other.theValue);
	}

	@Override
	public String toString() {
		return theAnchor + "." + theMetric;
	}

	static <T> ObservableChannel<T> create(Anchor a, Metric<T> m, ObservableValue<? extends T> value, Set<MetricTag> tags) {
		return new ObservableChannel<>(a, m, value, tags);
	}

	private static class ObservableResults<T> extends MetricQueryResult.AbstractMQR<T> {
		Subscription subscription;

		ObservableResults(ObservableChannel<T> channel) {
			super(channel);
		}

		@Override
		public ObservableChannel<T> getChannel() {
			return (ObservableChannel<T>) super.getChannel();
		}

		@Override
		public MetricResult<T> getResult(MetricResult<T> result, MetricQueryOptions options) {
			return result.setValue(getChannel().getCurrentValue());
		}

		@Override
		public Subscription notifyOnChange(Consumer<Object> onChange) {
			return getChannel().theValue.noInitChanges().act(onChange);
		}

		@Override
		public Transaction lock() {
			return getChannel().theValue.lock();
		}

		@Override
		public Transaction tryLock() {
			return getChannel().theValue.tryLock();
		}

		@Override
		public void checkDynamicDependencies() {
		}

		@Override
		public long getStamp() {
			return getChannel().theValue.getStamp();
		}

		@Override
		public boolean isValid() {
			return true;
		}

		@Override
		public void unsubscribe() {
			Subscription sub = subscription;
			if (sub != null) {
				subscription = null;
				sub.unsubscribe();
			}
		}
	}
}
