package org.metricity.metric.util.derived;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;

import org.metricity.metric.service.MetricChannel;
import org.metricity.metric.service.MetricQueryResult;
import org.observe.Subscription;
import org.qommons.Lockable;
import org.qommons.Stamped;
import org.qommons.Transaction;
import org.qommons.collect.ListenerList;

public abstract class CombinedMetricResults<T> extends MetricQueryResult.AbstractMQR<T> {
	private final int theDependencyCount;
	private final ListenerList<Consumer<Object>> theListeners;
	private final MetricQueryResult<?>[] theCachedDependencies;
	private final Subscription[] theDependencySubscriptions;

	public CombinedMetricResults(MetricChannel<T> channel, int dependencyCount) {
		super(channel);
		theDependencyCount = dependencyCount;
		theCachedDependencies = new MetricQueryResult[theDependencyCount];
		theDependencySubscriptions = new Subscription[theDependencyCount];
		theListeners = ListenerList.build().withFastSize(false).forEachSafe(false).withInUse(inUse -> {
			if (inUse) {
				synchronized (CombinedMetricResults.this) {
					for (int i = 0; i < theDependencyCount; i++) {
						theCachedDependencies[i] = getDependency(i);
						if (theCachedDependencies[i] != null)
							theDependencySubscriptions[i] = theCachedDependencies[i]
								.notifyOnChange(CombinedMetricResults.this::fireChange);
					}
				}
			} else {
				Subscription.forAll(theDependencySubscriptions).unsubscribe();
				Arrays.fill(theDependencySubscriptions, null);
			}
		}).build();
	}

	public int getDependencyCount() {
		return theDependencyCount;
	}

	protected abstract MetricQueryResult<?> getDependency(int dependency);

	protected abstract boolean isRequired(int dependency);

	protected Collection<MetricQueryResult<?>> getDependencies() {
		return new AbstractCollection<MetricQueryResult<?>>() {
			@Override
			public int size() {
				return theDependencyCount;
			}

			@Override
			public Iterator<MetricQueryResult<?>> iterator() {
				return new Iterator<MetricQueryResult<?>>() {
					private int theIndex;

					@Override
					public boolean hasNext() {
						return theIndex < theDependencyCount;
					}

					@Override
					public MetricQueryResult<?> next() {
						return getDependency(theIndex++);
					}
				};
			}
		};
	}

	@Override
	public Transaction lock() {
		return Lockable.lockAll(getDependencies());
	}

	@Override
	public Transaction tryLock() {
		return Lockable.tryLockAll(getDependencies());
	}

	@Override
	public long getStamp() {
		return Stamped.compositeStamp(getDependencies(), Stamped::getStamp);
	}

	@Override
	public synchronized Subscription notifyOnChange(Consumer<Object> onChange) {
		return theListeners.add(onChange, true)::run;
	}

	@Override
	public boolean isValid() {
		for (int i = 0; i < theDependencyCount; i++) {
			if (!isRequired(i)) {
				continue;
			}
			MetricQueryResult<?> result = getDependency(i);
			if (result != null && !result.isValid()) {
				return false;
			}
		}
		return true;
	}

	@Override
	public void checkDynamicDependencies() {
		if (!theListeners.isEmpty()) {
			synchronized (this) {
				for (int i = 0; i < theDependencyCount; i++) {
					MetricQueryResult<?> dep = getDependency(i);
					if (dep != theCachedDependencies[i]) {
						if (theDependencySubscriptions[i] != null)
							theDependencySubscriptions[i].unsubscribe();
						theDependencySubscriptions[i] = theCachedDependencies[i] == null ? null
							: theCachedDependencies[i].notifyOnChange(this::fireChange);
					}
				}
			}
		}
		for (int i = 0; i < theDependencyCount; i++) {
			MetricQueryResult<?> dep = theCachedDependencies[i];
			if (dep != null)
				dep.checkDynamicDependencies();
		}
	}

	@Override
	public void unsubscribe() {
		for (int i = 0; i < theDependencyCount; i++) {
			MetricQueryResult<?> result = getDependency(i);
			if (result != null) {
				result.unsubscribe();
			}
		}
	}

	synchronized void fireChange(Object cause) {
		theListeners.forEach(//
			listener -> listener.accept(cause));
	}
}
