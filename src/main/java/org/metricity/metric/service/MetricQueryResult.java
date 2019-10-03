package org.metricity.metric.service;

import java.util.function.Consumer;

import org.observe.Observable;
import org.observe.ObservableValue;
import org.observe.ObservableValueEvent;
import org.observe.Observer;
import org.observe.Subscription;
import org.observe.util.TypeTokens;
import org.qommons.Causable;
import org.qommons.Identifiable;
import org.qommons.Transaction;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

public interface MetricQueryResult<T> extends ObservableValue<MetricResult<T>>, Subscription {
	@SuppressWarnings("rawtypes")
	static TypeTokens.TypeKey<MetricQueryResult> TYPE_KEY = TypeTokens.get().keyFor(MetricQueryResult.class)//
		.enableCompoundTypes(new TypeTokens.UnaryCompoundTypeCreator<MetricQueryResult>() {
			@Override
			public <P> TypeToken<? extends MetricQueryResult> createCompoundType(TypeToken<P> param) {
				return new TypeToken<MetricQueryResult<P>>() {}.where(new TypeParameter<P>() {}, param);
			}
		});

	static <T> TypeToken<MetricQueryResult<T>> queryType(TypeToken<T> valueType) {
		return TYPE_KEY.getCompoundType(valueType);
	}

	MetricChannel<T> getChannel();

	@Override
	Transaction lock();

	@Override
	Transaction tryLock();

	void checkDynamicDependencies();

	boolean isValid();

	/**
	 * Populates the given {@link MetricResult} with the current status/value
	 * 
	 * @param result The result to populate
	 * @param options The options for the query
	 * @return The result parameter
	 */
	MetricResult<T> getResult(MetricResult<T> result, MetricQueryOptions options);

	Subscription notifyOnChange(Consumer<Object> onChange);

	@Override
	default MetricResult<T> get() {
		return getResult(new MetricResult<>(), MetricQueryOptions.get());
	}

	@Override
	default Observable<ObservableValueEvent<MetricResult<T>>> noInitChanges() {
		return changes().noInit();
	}

	default MetricResult<T> waitFor(long waitTime) {
		MetricResult<T> result = new MetricResult<>();
		long end = waitTime <= 0 ? 0 : System.currentTimeMillis() + waitTime;
		while (true) {
			result = getResult(result, MetricQueryOptions.get());
			if (!result.isComputing())
				return result;
			long waitNow;
			if (waitTime > 0) {
				waitNow = end - System.currentTimeMillis();
				if (waitNow <= 0)
					return result;
				else if (waitNow > 10)
					waitNow = 10;
			} else
				waitNow = 10;
			try {
				Thread.sleep(waitNow);
			} catch (InterruptedException e) {}
		}
	}

	@Override
	default Observable<ObservableValueEvent<MetricResult<T>>> changes() {
		return new Observable<ObservableValueEvent<MetricResult<T>>>() {
			private Object theIdentity;

			@Override
			public Object getIdentity() {
				if (theIdentity == null)
					theIdentity = Identifiable.wrap(MetricQueryResult.this.getIdentity(), "changes");
				return theIdentity;
			}

			@Override
			public Subscription subscribe(Observer<? super ObservableValueEvent<MetricResult<T>>> observer) {
				class ObserverNotification implements Consumer<Object> {
					private MetricResult<T> oldResult;
					private MetricResult<T> currentResult;

					@Override
					public void accept(Object cause) {
						fire(cause, false);
					}

					void fire(Object cause, boolean init) {
						MetricResult<T> newResult = getResult(oldResult, MetricQueryOptions.get());
						oldResult = currentResult;
						currentResult = newResult;
						ObservableValueEvent<MetricResult<T>> evt;
						if (init)
							evt = createInitialEvent(newResult, cause);
						else
							evt = createChangeEvent(oldResult, currentResult, cause);
						try (Transaction evtT = Causable.use(evt)) {
							observer.onNext(evt);
						}
					}
				}
				ObserverNotification notification = new ObserverNotification();
				try (Transaction t = lock()) {
					notification.fire(null, true);
					return notifyOnChange(notification);
				}
			}

			@Override
			public boolean isSafe() {
				return MetricQueryResult.this.isLockSupported();
			}

			@Override
			public Transaction lock() {
				return MetricQueryResult.this.lock();
			}

			@Override
			public Transaction tryLock() {
				return MetricQueryResult.this.tryLock();
			}
		};
	}

	/**
	 * Returns the value of the channel. If the data is not {@link MetricResult#isAvailable() available} yet, this method will block until
	 * it becomes available or updating is terminated.
	 * 
	 * @param options The options for the query
	 * @return The value for the time
	 */
	default T getValue(MetricQueryOptions options) {
		return getResult(new MetricResult<>(), options).get();
	}

	abstract class AbstractMQR<T> implements MetricQueryResult<T> {
		private final MetricChannel<T> theChannel;
		private TypeToken<MetricResult<T>> theType;
		private Object theIdentity;

		public AbstractMQR(MetricChannel<T> channel) {
			theChannel = channel;
		}

		@Override
		public TypeToken<MetricResult<T>> getType() {
			if (theType == null)
				theType = MetricResult.TYPE_KEY.getCompoundType(theChannel.getMetric().getType().getType());
			return theType;
		}

		@Override
		public Object getIdentity() {
			if (theIdentity == null)
				theIdentity = Identifiable.wrap(theChannel, "result");
			return theIdentity;
		}

		@Override
		public MetricChannel<T> getChannel() {
			return theChannel;
		}
	}

	static <T> MetricQueryResult<T> constant(MetricChannel<T> channel, T value) {
		return new AbstractMQR<T>(channel) {
			@Override
			public Transaction lock() {
				return Transaction.NONE;
			}

			@Override
			public Transaction tryLock() {
				return Transaction.NONE;
			}

			@Override
			public long getStamp() {
				return 0;
			}

			@Override
			public void checkDynamicDependencies() {
			}

			@Override
			public boolean isValid() {
				return true;
			}

			@Override
			public MetricResult<T> getResult(MetricResult<T> result, MetricQueryOptions options) {
				result.setValue(value);
				return result;
			}

			@Override
			public Subscription notifyOnChange(Consumer<Object> onChange) {
				return Subscription.NONE;
			}

			@Override
			public void unsubscribe() {}
		};
	}
}
