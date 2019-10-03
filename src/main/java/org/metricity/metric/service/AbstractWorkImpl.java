package org.metricity.metric.service;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.observe.Observable;
import org.qommons.collect.ListenerList;

abstract class AbstractWorkImpl implements MetricWork {
	final AbstractWorkImpl theParent;
	private final ListenerList<AbstractWorkImpl> theSubWorks;
	private final ListenerList<Consumer<Boolean>> theActivityListeners;

	private final DoubleValue theAmount;
	private Runnable theParentHook;

	AbstractWorkImpl(boolean safe) {
		this(null, safe, null);
	}

	AbstractWorkImpl(AbstractWorkImpl parent, Observable<?> unsubscribe) {
		this(parent, parent.theAmount.isSafe(), unsubscribe);
	}

	private AbstractWorkImpl(AbstractWorkImpl parent, boolean safe, Observable<?> unsubscribe) {
		theParent = parent;
		theSubWorks = new ListenerList<>(null, false, null);
		theActivityListeners = new ListenerList<>(null, false, null);
		theAmount = safe ? new SafeDouble() : new SimpleDouble();

		if (theParent != null) {
			theParentHook=theParent.theSubWorks.add(this, true);
			if (unsubscribe != null) {
				unsubscribe.take(1).act(v -> {
					theParentHook.run();
					theParentHook = null;
				});
			}
		}
	}

	protected void workQueued(MetricChannel<?> channel, double weight) {
		if (theAmount.add(weight)) {
			activeChanged(true);
		}
		theSubWorks.forEach(//
				sub -> sub.workQueued(channel, weight));
	}

	protected void workFinished(MetricChannel<?> channel, double weight) {
		if (theAmount.remove(weight)) {
			activeChanged(false);
		}
		theSubWorks.forEach(//
				sub -> sub.workFinished(channel, weight));
	}

	private final void activeChanged(Boolean active) {
		theActivityListeners.forEach(//
				l -> l.accept(active));
	}

	@Override
	public boolean isActive() {
		return theAmount.isActive();
	}

	@Override
	public int getCurrentJobCount() {
		return (int) getCurrentJobs().count();
	}

	@Override
	public final double getWaitingWorkAmount() {
		if (theParent != null && theParentHook == null) {
			throw new IllegalStateException("This sub-work has been unsubscribed and is no longer valid");
		}
		return theAmount.get();
	}

	@Override
	public final Runnable addActiveListener(Consumer<Boolean> active) {
		return theActivityListeners.add(active, true);
	}

	// @Override
	// public final MetricWork filter(Predicate<? super MetricChannel<?>> filter, Observable<?> until) {
	// return new SubMetricWork(this, filter, until);
	// }

	static final class SubMetricWork extends AbstractWorkImpl {
		private final Predicate<? super MetricChannel<?>> theFilter;

		SubMetricWork(AbstractWorkImpl parent, Predicate<? super MetricChannel<?>> filter, Observable<?> until) {
			super(parent, until);
			theFilter = filter;
		}

		@Override
		public Stream<MetricChannel<?>> getCurrentJobs() {
			return theParent.getCurrentJobs().filter(theFilter);
		}

		@Override
		protected void workQueued(MetricChannel<?> channel, double weight) {
			if (channel == null || theFilter.test(channel)) {
				super.workQueued(channel, weight);
			}
		}

		@Override
		protected void workFinished(MetricChannel<?> channel, double weight) {
			if (channel == null || theFilter.test(channel)) {
				super.workFinished(channel, weight);
			}
		}
	}

	interface DoubleValue {
		boolean isActive();

		double get();

		boolean add(double value);

		boolean remove(double value);

		boolean isSafe();
	}

	static final class SimpleDouble implements DoubleValue {
		private long theCount;
		private double theWeight;

		@Override
		public boolean isActive() {
			return theCount != 0L;
		}

		@Override
		public double get() {
			if (theCount == 0L) {
				return 0.0;
			}
			double w = theWeight;
			if (w == 0.0) {
				w = Double.MIN_VALUE;
			}
			return w;
		}

		@Override
		public boolean add(double value) {
			boolean newlyActive = theCount == 0L;
			theCount++;
			theWeight += value;
			return newlyActive;
		}

		@Override
		public boolean remove(double value) {
			if (theCount <= 1L) {
				theCount = 0L;
				theWeight = 0.0;
			} else {
				double w = theWeight - value;
				if (w < 0) {
					w = 0;
				}
				theCount--;
			}
			return theCount == 0L;
		}

		@Override
		public boolean isSafe() {
			return false;
		}
	}

	static final class SafeDouble implements DoubleValue {
		private final AtomicReference<long[]> theCountAndWeight;

		SafeDouble() {
			theCountAndWeight = new AtomicReference<>(new long[2]);
		}

		@Override
		public boolean isActive() {
			return theCountAndWeight.get()[0] != 0L;
		}

		@Override
		public double get() {
			long[] caw = theCountAndWeight.get();
			if (caw[0] == 0L) {
				return 0.0;
			}
			double w = Double.longBitsToDouble(caw[1]);
			if (w < 0.0) {
				w = Double.MIN_VALUE;
			}
			return w;
		}

		@Override
		public boolean add(double value) {
			long[] oldValue = theCountAndWeight.getAndUpdate(caw -> {
				long[] newCaw = new long[2];
				if (caw[0] == 0L) {
					newCaw[0] = 1L;
					newCaw[1] = Double.doubleToLongBits(value);
				} else {
					newCaw[0] = caw[0] + 1L;
					newCaw[1] = Double.doubleToLongBits(Double.longBitsToDouble(caw[1]) + value);
				}
				return newCaw;
			});
			return oldValue[0] == 0L;
		}

		boolean belowZero;

		@Override
		public boolean remove(double value) {
			long[] newValue = theCountAndWeight.updateAndGet(caw -> {
				long[] newCaw = new long[2];
				if (caw[0] <= 0L) {
					// The values stay at zero
					belowZero = true;
				} else if (caw[0] == 0L) {
				} else {
					newCaw[0] = caw[0] - 1L;
					double newWeight = Double.longBitsToDouble(caw[1]) - value;
					newCaw[1] = Double.doubleToLongBits(newWeight);
				}
				return newCaw;
			});
			if (belowZero) {
				belowZero = false;
				System.err.println("Below zero!!");
			}
			return newValue[0] == 0L;
		}

		@Override
		public boolean isSafe() {
			return true;
		}
	}
}