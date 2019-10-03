package org.metricity.metric.service;

import java.util.Objects;

import org.observe.util.TypeTokens;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

/**
 * <p>
 * A <b>mutable<b> structure passed into and returned from a metric query that stores the value result or computation state that is the
 * result of the query.
 * </p>
 * <p>
 * The use of this structure allows the metric API to be queried once to answer any of several questions that the API needs to answer:
 * <ul>
 * <li>What is the value for a sample?</li>
 * <li>Is computation on a metric value complete?</li>
 * <li>Can I expect a metric value later?</li>
 * </ul>
 * It also allows the architecture to pass extra data around internally.
 * </p>
 * <p>
 * The structure is mutable in order that it may be reused for an entire query of a top-level metric and its dependencies instead of needing
 * to create a new structure to return each dependency value/state. {@link #immutable() Immutable} copies can be made.
 * </p>
 * 
 * @param <T> The type of the value
 */
public class MetricResult<T> {
	/** The {@link TypeTokens} key for this class */
	@SuppressWarnings("rawtypes")
	public static TypeTokens.TypeKey<MetricResult> TYPE_KEY = TypeTokens.get().keyFor(MetricResult.class)//
		.enableCompoundTypes(new TypeTokens.UnaryCompoundTypeCreator<MetricResult>() {
			@Override
			public <P> TypeToken<? extends MetricResult> createCompoundType(TypeToken<P> param) {
				return new TypeToken<MetricResult<P>>() {}.where(new TypeParameter<P>() {}, param);
			}
		});

	private static final int AVAILABLE = 0b1;
	private static final int COMPUTING = 0b10;
	private static final int INVALID = 0b100;
	private static final int NO_CACHE = 0b1000;

	private static final ImmutableMetricResult<Object> INVALID_RESULT = new MetricResult<>().invalid().immutable();
	private static final ImmutableMetricResult<Object> COMPUTING_RESULT = new MetricResult<>().unavailable(true).immutable();

	/**
	 * @param <T> The type of the value
	 * @return An invalid result
	 */
	public static <T> ImmutableMetricResult<T> INVALID() {
		return (ImmutableMetricResult<T>) INVALID_RESULT;
	}

	/**
	 * @param <T> The type of the value
	 * @return A computing result
	 */
	public static <T> ImmutableMetricResult<T> COMPUTING() {
		return (ImmutableMetricResult<T>) COMPUTING_RESULT;
	}

	private T theValue;
	private int theStatus;

	/** Creates a new metric result */
	public MetricResult() {}

	/**
	 * Creates a metric result, copying state from the given result
	 * 
	 * @param toCopy The result to copy
	 */
	public MetricResult(MetricResult<T> toCopy) {
		theValue = toCopy.theValue;
		theStatus = toCopy.theStatus;
	}

	/** @return The metric value */
	public T get() {
		return theValue;
	}

	/** @return Whether the result was valid */
	public boolean isValid() {
		return !getStatus(INVALID);
	}

	/** @return Whether the result was an available value */
	public boolean isAvailable() {
		return getStatus(AVAILABLE);
	}

	/** @return Whether computation is either scheduled or executing for the value */
	public boolean isComputing() {
		return getStatus(COMPUTING);
	}

	private boolean getStatus(int mask) {
		return (theStatus & mask) != 0;
	}

	/** @return Whether this structure is mutable */
	public boolean isMutable() {
		return true;
	}

	/** @return Whether this result can be cached and returned for future requests */
	public boolean isCachable() {
		return !getStatus(NO_CACHE);
	}

	/** @return An immutable result with this result's state */
	public ImmutableMetricResult<T> immutable() {
		return new ImmutableMetricResult<>(this);
	}

	/**
	 * @param other The result to copy
	 * @return This result
	 */
	public MetricResult<T> setState(MetricResult<T> other) {
		theValue = other.theValue;
		theStatus = other.theStatus;
		return this;
	}

	/**
	 * @param value The value to set
	 * @return This result
	 */
	public MetricResult<T> setValue(T value) {
		theValue = value;
		theStatus = AVAILABLE;
		return this;
	}

	/**
	 * @param cacheable Whether this result should be cacheable
	 * @return This result
	 */
	public MetricResult<T> cacheable(boolean cacheable) {
		if (cacheable) {
			theStatus &= ~NO_CACHE;
		} else {
			theStatus |= NO_CACHE;
		}
		return this;
	}

	/**
	 * Sets this result as uncacheable
	 * 
	 * @return This result
	 */
	public MetricResult<T> uncacheable() {
		theStatus |= NO_CACHE;
		return this;
	}

	/**
	 * Sets this result as unavailable
	 * 
	 * @param computing Whether this result should indicate that computation is scheduled or currently executing
	 * @return This result
	 */
	public MetricResult<T> unavailable(boolean computing) {
		theValue = null;
		theStatus = computing ? COMPUTING : 0;
		return this;
	}

	/**
	 * Sets this result as invalid
	 * 
	 * @return This result
	 */
	public MetricResult<T> invalid() {
		theValue = null;
		theStatus = INVALID;
		return this;
	}

	/**
	 * Gets the value of this result, with a default value if it is not available
	 * 
	 * @param defValue The default value
	 * @return The value
	 */
	public T getWithDefault(T defValue) {
		if (isAvailable()) {
			return theValue;
		} else {
			return defValue;
		}
	}

	/**
	 * Clears this result's state
	 * 
	 * @return This result
	 */
	public MetricResult<T> clear() {
		theValue = null;
		theStatus = 0;
		return this;
	}

	/**
	 * Reuses this result for a different type
	 * 
	 * @param <X> The type to reuse as
	 * @return This result, as a different type
	 */
	public <X> MetricResult<X> reuse() {
		return (MetricResult<X>) this;
	}

	/** @return A mutable copy of this result */
	public MetricResult<T> copy() {
		MetricResult<T> copy = new MetricResult<>(this);
		return copy;
	}

	@Override
	public int hashCode() {
		if (isAvailable()) {
			return Objects.hashCode(theValue);
		} else {
			return theStatus;
		}
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		} else if (!(o instanceof MetricResult)) {
			return false;
		}
		MetricResult<?> other = (MetricResult<?>) o;
		if (theStatus != other.theStatus) {
			return false;
		}
		if (isAvailable() && !Objects.equals(theValue, other.theValue)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		if (isAvailable()) {
			String str = String.valueOf(theValue);
			if (!isCachable()) {
				str += " (uncacheable)";
			}
			return str;
		} else if (isComputing()) {
			return "Computing";
		} else if (isValid()) {
			return "Unavailable";
		} else {
			return "Invalid";
		}
	}

	/**
	 * An immutable result
	 * 
	 * @param <T> The type of the value
	 */
	public static class ImmutableMetricResult<T> extends MetricResult<T> {
		/**
		 * @param result The result to copy the state of
		 */
		public ImmutableMetricResult(MetricResult<T> result) {
			super(result);
		}

		@Override
		public boolean isMutable() {
			return false;
		}

		@Override
		public ImmutableMetricResult<T> immutable() {
			return this; // Already immutable
		}

		@Override
		public MetricResult<T> setState(MetricResult<T> other) {
			throw new IllegalStateException("Immutable");
		}

		@Override
		public MetricResult<T> setValue(T value) {
			throw new IllegalStateException("Immutable");
		}

		@Override
		public MetricResult<T> unavailable(boolean computing) {
			throw new IllegalStateException("Immutable");
		}

		@Override
		public MetricResult<T> invalid() {
			throw new IllegalStateException("Immutable");
		}
	}
}
