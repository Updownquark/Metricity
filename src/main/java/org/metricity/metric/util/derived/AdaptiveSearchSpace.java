package org.metricity.metric.util.derived;

import java.util.Iterator;
import java.util.function.BiPredicate;
import java.util.function.Function;

import org.dbug.DBug;
import org.dbug.DBugAnchor;
import org.dbug.DBugAnchorType;
import org.metricity.metric.service.MetricChannel;
import org.metricity.metric.service.MetricResult;
import org.observe.util.TypeTokens;
import org.qommons.collect.BetterSortedSet.SortedSearchFilter;
import org.qommons.tree.BinaryTreeNode;
import org.qommons.tree.TreeBasedList;
import org.qommons.tree.TreeBasedSortedList;

import com.google.common.reflect.TypeToken;

/**
 * A search space for adapting a metric value from another
 * 
 * @param <X> The adaptive metric type
 * @param <T> The adapted metric type
 */
public interface AdaptiveSearchSpace<X, T> {
	/**
	 * @param result The result to populate
	 * @return The result containing the currently adapted value in the search space
	 */
	MetricResult<T> getCurrentValue(MetricResult<T> result);

	/**
	 * @param targetValue The adaptive metric value to adapt against. This is the result of evaluated the adaptive metric against the
	 *        current adapted metric value.
	 * @return The result containing the new adapted value
	 */
	MetricResult<T> adapt(MetricResult<X> targetValue);

	/** @return Whether this search is complete */
	boolean isComplete();

	/**
	 * Creates a search space that does no adaptation and returns a single value
	 * 
	 * @param <X> The adaptive metric type
	 * @param <T> The adapted metric type
	 * @param value The adapted value
	 * @return The single-value search space
	 */
	public static <X, T> AdaptiveSearchSpace<X, T> singleValue(T value) {
		return new SingleValueSearchSpace<>(value);
	}

	public interface AdaptiveResultComparator<E, X> {
		int compare(E src1, X res1, E src2, X res2);
	}

	public interface AdaptiveResultSearch<E, X> {
		int compareTo(E src, X res);
	}

	// On second thought, I think maybe this doesn't make any sense. If A depends on B, how can B just be a straight function of A?
	// public static <X, T> AdaptiveSearchSpace<X, T> map(T initValue, Function<? super X, ? extends T> map) {
	// return new SimpleMappingSearchSpace<>(initValue, map);
	// }

	/**
	 * Creates a search space that simply iterates through a sequence of adapted values and returns the one that produces the best adaptive
	 * value
	 * 
	 * @param <E> The type of value in the sequence
	 * @param <T> The type of the adapted value
	 * @param values The sequence of values to try
	 * @param map The function to produce adapted values from each sequence value
	 * @return A builder to produce the search space
	 */
	public static <E, T> ExhaustiveSearchSpaceBuilder<E, T> exhaustiveSearch(Iterable<E> values, Function<? super E, ? extends T> map) {
		return new ExhaustiveSearchSpaceBuilder<>(values, map);
	}

	public static <E, T> TreeSearchSpaceBuilder<E, T> treeSearch(TreeBasedSortedList<E> values, Function<? super E, ? extends T> map) {
		return new TreeSearchSpaceBuilder<>(values, map);
	}

	/**
	 * Builds a search space that simply iterates through a sequence of adapted values and returns the one that produces the best adaptive
	 * value
	 * 
	 * @param <E> The type of value in the sequence
	 * @param <T> The type of the adapted value
	 */
	public class ExhaustiveSearchSpaceBuilder<E, T> {
		private final Iterable<E> theValues;
		private final Function<? super E, ? extends T> theMap;

		public ExhaustiveSearchSpaceBuilder(Iterable<E> values, Function<? super E, ? extends T> map) {
			theValues = values;
			theMap = map;
		}

		/**
		 * @param <X> The type of the adaptive metric
		 * @param compare The comparison for the sequence/adaptive tuples
		 * @return The search space
		 */
		public <X> AdaptiveSearchSpace<X, T> build(AdaptiveResultComparator<? super E, ? super X> compare) {
			return build(compare, null);
		}

		/**
		 * @param <X> The type of the adaptive metric
		 * @param compare The comparison for the sequence/adaptive tuples
		 * @param stop A function that can stop the iteration when a value that is known to be unsurpassable is reached
		 * @return The search space
		 */
		public <X> AdaptiveSearchSpace<X, T> build(AdaptiveResultComparator<? super E, ? super X> compare,
			BiPredicate<? super E, ? super X> stop) {
			return new ExhaustiveSearchSpace<>(theValues.iterator(), theMap, compare, stop);
		}
	}

	public class TreeSearchSpaceBuilder<E, T> {
		private final TreeBasedSortedList<E> theValues;
		private final Function<? super E, ? extends T> theMap;
		private BinaryTreeNode<E> theInitialValue;

		TreeSearchSpaceBuilder(TreeBasedSortedList<E> values, Function<? super E, ? extends T> map) {
			theValues = values;
			theMap = map;
			theInitialValue = values.getRoot();
		}

		public TreeSearchSpaceBuilder<E, T> withInitialValue(Function<TreeBasedList<E>, BinaryTreeNode<E>> initialValue) {
			theInitialValue = initialValue.apply(theValues);
			return this;
		}

		public TreeSearchSpaceBuilder<E, T> withInitialValue(Comparable<? super E> initialValue) {
			theInitialValue = theValues.search(initialValue, SortedSearchFilter.PreferLess);
			return this;
		}

		public <X> AdaptiveSearchSpace<X, T> build(AdaptiveResultSearch<? super E, ? super X> compare,
			AdaptiveResultComparator<? super E, ? super X> choice) {
			return new TreeSearchSpace<>(theValues, theMap, theInitialValue, compare, choice);
		}
	}

	class SingleValueSearchSpace<X, T> implements AdaptiveSearchSpace<X, T> {
		private final T theValue;

		public SingleValueSearchSpace(T value) {
			theValue = value;
		}

		@Override
		public MetricResult<T> getCurrentValue(MetricResult<T> result) {
			return result.setValue(theValue);
		}

		@Override
		public MetricResult<T> adapt(MetricResult<X> targetValue) {
			return targetValue.<T> reuse().setValue(theValue);
		}

		@Override
		public boolean isComplete() {
			return true;
		}
	}

	class SimpleMappingSearchSpace<X, T> implements AdaptiveSearchSpace<X, T> {
		private T theValue;
		private final Function<? super X, ? extends T> theMap;
		private boolean isComplete;

		public SimpleMappingSearchSpace(T initialValue, Function<? super X, ? extends T> map) {
			theValue = initialValue;
			theMap = map;
		}

		@Override
		public MetricResult<T> getCurrentValue(MetricResult<T> result) {
			return result.setValue(theValue);
		}

		@Override
		public MetricResult<T> adapt(MetricResult<X> targetValue) {
			isComplete = true;
			theValue = theMap.apply(targetValue.get());
			return targetValue.<T> reuse().setValue(theValue);
		}

		@Override
		public boolean isComplete() {
			return isComplete;
		}
	}

	class ExhaustiveSearchSpace<E, X, T> implements AdaptiveSearchSpace<X, T> {
		private final Iterator<E> theValues;
		private final Function<? super E, ? extends T> theMap;
		private final AdaptiveResultComparator<? super E, ? super X> theCompare;
		private final BiPredicate<? super E, ? super X> theStop;

		private E theLastValue;
		private boolean hasBest;
		private E theBestValue;
		private X theBestResult;
		private boolean isComplete;

		private final DBugAnchor<AdaptiveSearchSpace<?, ?>> debug;

		public ExhaustiveSearchSpace(Iterator<E> values, Function<? super E, ? extends T> map,
			AdaptiveResultComparator<? super E, ? super X> compare, BiPredicate<? super E, ? super X> stop) {
			super();
			theValues = values;
			theMap = map;
			theCompare = compare;
			theStop = stop;

			theLastValue = values.hasNext() ? values.next() : null;

			debug = DEBUG_TYPE.debug(this).buildIfSatisfied();
		}

		@Override
		public MetricResult<T> getCurrentValue(MetricResult<T> result) {
			return result.setValue(theMap.apply(theLastValue));
		}

		@Override
		public MetricResult<T> adapt(MetricResult<X> targetValue) {
			if (isComplete) {
				return getCurrentValue(targetValue.reuse());
			} else if (hasBest) {
				int comp = theCompare.compare(theLastValue, targetValue.get(), theBestValue, theBestResult);
				debug.event("adapt").with("bestSrc", theBestValue).with("bestRes", theBestResult)//
					.with("newSrc", theLastValue).with("newRes", targetValue.get())//
					.with("compare", comp).occurred();
				if (comp < 0) {
					theBestValue = theLastValue;
					theBestResult = targetValue.get();
				}
			} else {
				hasBest = true;
				theBestValue = theLastValue;
				theBestResult = targetValue.get();
			}
			if (theValues.hasNext()) {
				if (theStop != null && theStop.test(theBestValue, theBestResult)) {
					debug.event("stopped").with("bestSrc", theBestValue).with("bestRes", theBestResult).occurred();
					theLastValue = theBestValue;
					isComplete = true;
					return targetValue.<T> reuse().setValue(theMap.apply(theBestValue));
				} else {
					theLastValue = theValues.next();
					return targetValue.<T> reuse().setValue(theMap.apply(theLastValue));
				}
			} else {
				theLastValue = theBestValue;
				isComplete = true;
				return targetValue.<T> reuse().setValue(theMap.apply(theBestValue));
			}
		}

		@Override
		public boolean isComplete() {
			return isComplete;
		}
	}

	class TreeSearchSpace<E, X, T> implements AdaptiveSearchSpace<X, T> {
		private final TreeBasedSortedList<E> theValues;
		private final Function<? super E, ? extends T> theMap;
		private final AdaptiveResultSearch<? super E, ? super X> theCompare;
		private final AdaptiveResultComparator<? super E, ? super X> theChoice;

		private BinaryTreeNode<E> theLeftBound;
		private boolean hasLeftResult;
		private X theLeftResult;
		private BinaryTreeNode<E> theRightBound;
		private boolean hasRightResult;
		private X theRightResult;
		private BinaryTreeNode<E> theLastValue;

		public TreeSearchSpace(TreeBasedSortedList<E> values, Function<? super E, ? extends T> map, BinaryTreeNode<E> initialValue,
			AdaptiveResultSearch<? super E, ? super X> compare, AdaptiveResultComparator<? super E, ? super X> choice) {
			theValues = values;
			theMap = map;
			theLastValue = initialValue;
			theCompare = compare;
			theChoice = choice;

			theLeftBound = values.getTerminalElement(true);
			theRightBound = values.getTerminalElement(false);
		}

		@Override
		public MetricResult<T> getCurrentValue(MetricResult<T> result) {
			return result.setValue(theMap.apply(theLastValue.get()));
		}

		@Override
		public MetricResult<T> adapt(MetricResult<X> targetValue) {
			if (theValues.isEmpty()) {
				return null;
			}
			int compare = theCompare.compareTo(theLastValue.get(), targetValue.get());
			if (compare == 0) {
				theLastValue = theLeftBound = theRightBound = theLastValue;
				theLeftResult = theRightResult = null;
				return targetValue.<T> reuse().setValue(theMap.apply(theLastValue.get()));
			} else if (compare < 0) {
				hasLeftResult = true;
				theLeftBound = theLastValue;
				theLeftResult = targetValue.get();
			} else {
				hasRightResult = true;
				theRightBound = theLastValue;
				theRightResult = targetValue.get();
			}
			if (theLeftBound.getElementId().equals(theRightBound.getElementId())) {
				theLastValue = theLeftBound;
				theLeftResult = theRightResult = null;
				return targetValue.<T> reuse().setValue(theMap.apply(theLastValue.get()));
			}
			BinaryTreeNode<E> between = theValues.splitBetween(theLeftBound.getElementId(), theRightBound.getElementId());
			if (between == null) {
				// left and right are adjacent
				if (!hasLeftResult) {
					theLastValue = theLeftBound;
				} else if (!hasRightResult) {
					theLastValue = theRightBound;
				} else {
					int choice = theChoice.compare(theLeftBound.get(), theLeftResult, theRightBound.get(), theRightResult);
					if (choice <= 0) {
						theRightBound = theLeftBound;
					} else {
						theLeftBound = theRightBound;
					}
					theLastValue = theLeftBound;
					theLeftResult = theRightResult = null;
				}
				return targetValue.<T> reuse().setValue(theMap.apply(theLastValue.get()));
			}
			theLastValue = between;
			return targetValue.<T> reuse().setValue(theMap.apply(theLastValue.get()));
		}

		@Override
		public boolean isComplete() {
			if (theValues.isEmpty()) {
				return true;
			}
			return theLeftBound != null && theRightBound != null && theLeftBound.getElementId().equals(theRightBound.getElementId());
		}
	}

	DBugAnchorType<AdaptiveSearchSpace<?, ?>> DEBUG_TYPE = DBug.declare("metricity",
		(Class<AdaptiveSearchSpace<?, ?>>) (Class<?>) AdaptiveSearchSpace.class, b -> b//
			.withExternalStaticField("channel", new TypeToken<MetricChannel<?>>() {})//
			.withEvent("adapt", eb -> eb//
				.withEventField("bestSrc", TypeTokens.get().OBJECT).withEventField("bestRes", TypeTokens.get().OBJECT)//
				.withEventField("newSrc", TypeTokens.get().OBJECT).withEventField("newRes", TypeTokens.get().OBJECT)//
				.withEventField("compare", TypeTokens.get().INT))
			.withEvent("stopped", eb -> eb//
				.withEventField("bestSrc", TypeTokens.get().OBJECT).withEventField("bestRes", TypeTokens.get().OBJECT)));
}