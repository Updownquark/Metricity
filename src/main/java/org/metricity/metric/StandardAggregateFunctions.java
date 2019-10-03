package org.metricity.metric;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.observe.util.TypeTokens;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

/** Contains {@link MetricAggregation}s and aggregation utilities recognized by the core metric architecture */
public class StandardAggregateFunctions {
	/** The name of the {@link #flatten(TypeToken, boolean) flatten} (non-distinct) aggregation */
	public static final String FLATTEN = "FLATTEN";
	/** The name of the {@link #distinct(TypeToken) distinct} aggregation */
	public static final String DISTINCT = "DISTINCT";
	/** The name of the {@link #flatten(TypeToken, boolean) flatten} (distinct) aggregation */
	public static final String FLATTEN_DISTINCT = FLATTEN + DISTINCT;
	/** The name of the {@link #collect(TypeToken) collect} aggregation */
	public static final String COLLECT = "COLLECT";

	private static final Set<String> ALL_STANDARD_AGGREGATIONS = new HashSet<>();

	static {
		ALL_STANDARD_AGGREGATIONS.addAll(Arrays.asList(//
			"MIN", "MAX", "MEAN", "SUM", "COUNT", "AMOUNT", COLLECT, FLATTEN, FLATTEN_DISTINCT));
	}

	private StandardAggregateFunctions() {}

	/** Counts items in a collection */
	public static final MetricAggregation<Object, Integer> COUNT = genericAgg("COUNT", values -> values.size(), TypeTokens.get().INT, 0);
	/** Counts true booleans in a collection of booleans and returns the proportion (0 to 1) that are true */
	public static final MetricAggregation<Boolean, Double> AMOUNT = booleanAgg("AMOUNT", 1E-10, values -> {
		int[] numTrueAndCount = new int[2];
		values.forEach(v -> {
			if (v) {
				numTrueAndCount[0]++;
			}
			numTrueAndCount[1]++;
		});
		return numTrueAndCount[0] * 1.0 / numTrueAndCount[1];
	});

	private static <T> MetricAggregation<Object, T> genericAgg(String name, Function<Collection<?>, T> agg, TypeToken<T> type,
		double cost) {
		MetricAggregation<Object, T> aggregation = new MetricAggregationImpl<>(name, type, cost, //
			values -> agg.apply(values));
		return aggregation;
	}

	private static MetricAggregation<Boolean, Double> booleanAgg(String name, double cost,
		Function<Stream<? extends Boolean>, Double> agg) {
		MetricAggregation<Boolean, Double> aggregation = new MetricAggregationImpl<>(name, TypeTokens.get().DOUBLE, cost, //
			values -> agg.apply(values.stream()));
		return aggregation;
	}

	private static class MetricAggregationImpl<T, X> implements MetricAggregation<T, X> {
		private final String theName;
		private final TypeToken<X> theType;
		private final double theCost;
		private final Function<Collection<? extends T>, X> theCollector;

		MetricAggregationImpl(String name, TypeToken<X> type, double cost, Function<Collection<? extends T>, X> collector) {
			theName = name;
			theType = type;
			theCost = cost;
			theCollector = collector;
		}

		@Override
		public X aggregate(Collection<? extends T> values) {
			return theCollector.apply(values);
		}

		@Override
		public TypeToken<X> getType() {
			return theType;
		}

		@Override
		public double getCost() {
			return theCost;
		}

		@Override
		public int hashCode() {
			// Going on the assumption here that the function of a standard aggregation is identified by its name
			return theName.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof MetricAggregationImpl && theName.equals(((MetricAggregationImpl<?, ?>) obj).theName);
		}

		@Override
		public String toString() {
			return theName;
		}
	}

	/**
	 * @param <T> The type of the metric to aggregate
	 * @param name The name of the standard aggregation function
	 * @param metric The metric to aggregate values of
	 * @return The standard metric aggregation with the given name, applying to the given metric's values
	 * @throws IllegalArgumentException If no such standard aggregation is recognized
	 */
	@SuppressWarnings("rawtypes")
	public static <T> MetricAggregation<? super T, ?> getAggregation(String name, MetricType<T> metric) throws IllegalArgumentException {
		boolean distinct = FLATTEN_DISTINCT.equals(name);
		if (distinct || FLATTEN.equals(name) && Collection.class.isAssignableFrom(TypeTokens.getRawType(metric.getType()))) {
			return (MetricAggregation<? super T, ?>) flatten(metric.getType().resolveType(Collection.class.getTypeParameters()[0]),
				distinct);
		} else if (COLLECT.equals(name)) {
			return collect(metric.getType());
		}
		MetricAggregation<?, ?> agg = null;
		Class<?> wrappedRaw = TypeTokens.get().wrap(TypeTokens.getRawType(metric.getType()));
		if ("SUM".equals(name)) {
			agg = sum((MetricType<? extends Number>) metric);
		} else if ("MEAN".equals(name)) {
			agg = mean((MetricType<? extends Number>) metric);
		}
		if (agg == null && Comparable.class.isAssignableFrom(wrappedRaw)) {
			// Generic nightmares
			if ("MAX".equals(name)) {
				agg = max((MetricType) metric);
			} else if ("MIN".equals(name)) {
				agg = min((MetricType) metric);
			}
		}
		if (agg == null && Boolean.class.equals(wrappedRaw)) {
			if (name.equals("AMOUNT")) {
				agg = AMOUNT;
			}
		}
		if (agg == null) {
			if (name.equals("COUNT")) {
				agg = COUNT;
			}
		}
		if (agg == null) {
			throw new IllegalArgumentException("No such aggregation " + name + " applies to type " + metric);
		}
		return (MetricAggregation<? super T, ?>) agg;
	}

	/**
	 * @param aggName The name of the aggregation
	 * @return Whether a standard aggregation exists with the given name
	 */
	public static boolean isStandard(String aggName) {
		return ALL_STANDARD_AGGREGATIONS.contains(aggName);
	}

	/**
	 * @param <T> The type of the metric
	 * @param metric The metric to aggregate values for
	 * @return All standard metric aggregations that may apply to the given metric
	 */
	@SuppressWarnings("rawtypes")
	public static <T> List<MetricAggregation<T, ?>> getStandardAggregations(MetricType<T> metric) {
		List<MetricAggregationImpl<T, ?>> aggs = new ArrayList<>();
		Class<?> wrappedRaw = TypeTokens.get().wrap(TypeTokens.getRawType(metric.getType()));
		if (Comparable.class.isAssignableFrom(wrappedRaw)) {
			aggs.add((MetricAggregationImpl<T, ?>) min((MetricType) metric));
			aggs.add((MetricAggregationImpl<T, ?>) max((MetricType) metric));
		}
		if (Number.class.isAssignableFrom(wrappedRaw)) {
			aggs.add((MetricAggregationImpl<T, ?>) sum((MetricType<? extends Number>) metric));
			aggs.add((MetricAggregationImpl<T, ?>) mean((MetricType<? extends Number>) metric));
		}
		if (Collection.class.isAssignableFrom(wrappedRaw)) {
			aggs.add((MetricAggregationImpl<T, ?>) flatten(metric.getType(), false));
			aggs.add((MetricAggregationImpl<T, ?>) flatten(metric.getType(), true));
		}
		aggs.add((MetricAggregationImpl<T, ?>) collect(metric.getType()));
		return Collections.unmodifiableList(aggs);
	}

	/**
	 * @param <T> The type of the aggregation
	 * @param type The type of the aggregation
	 * @return A simple aggregation that merely returns the collection of input values
	 */
	public static <T> MetricAggregation<T, Collection<T>> collect(TypeToken<T> type) {
		return new MetricAggregationImpl<>(COLLECT, new TypeToken<Collection<T>>() {}.where(new TypeParameter<T>() {}, type), 0,
			c -> (Collection<T>) c);
	}

	/**
	 * @param <T> The type of the aggregation
	 * @param type The type of the aggregation
	 * @return A simple aggregation that merely returns the collection of input values, as a list
	 */
	public static <T> MetricAggregation<T, List<T>> collectAsList(TypeToken<T> type) {
		return new MetricAggregationImpl<>(COLLECT, new TypeToken<List<T>>() {}.where(new TypeParameter<T>() {}, type), 0, c -> {
			if (c instanceof List) {
				return (List<T>) c;
			} else {
				ArrayList<T> list = new ArrayList<>(c.size());
				list.addAll(c);
				return list;
			}
		});
	}

	/**
	 * @param <T> The type of the input values
	 * @param <C> The type of the result collection
	 * @param type The type of the input values
	 * @param distinct Whether to eliminate duplicates in the result
	 * @return An aggregation that accepts collections as input and returns a flattened collection with all the values in the input
	 *         collections
	 */
	public static <T, C extends Collection<T>> MetricAggregation<Collection<? extends T>, C> flatten(TypeToken<T> type, boolean distinct) {
		TypeToken<C> resultType;
		if (distinct) {
			resultType = (TypeToken<C>) new TypeToken<Set<T>>() {};
		} else {
			resultType = (TypeToken<C>) new TypeToken<List<T>>() {};
		}
		resultType = resultType.where(new TypeParameter<T>() {}, type);
		Supplier<Collector<T, ?, C>> collector = () -> (Collector<T, ?, C>) (distinct ? Collectors.toSet() : Collectors.toList());
		Function<Collection<? extends Collection<? extends T>>, C> c = values -> values.stream().flatMap(//
			v -> v.stream()).collect(collector.get());
		return new MetricAggregationImpl<>(FLATTEN, resultType, 1E-9, c);
	}

	/**
	 * Like {@link #collect(TypeToken)}, but eliminates duplicates
	 * 
	 * @param <T> The type of values in the input
	 * @param type The type of values in the input
	 * @return A simple aggregation that merely returns the collection of input values with duplicates removed
	 */
	public static <T> MetricAggregation<? super T, Set<T>> distinct(TypeToken<T> type) {
		return new MetricAggregationImpl<>(DISTINCT, new TypeToken<Set<T>>() {}.where(new TypeParameter<T>() {}, type), 1E-9, c -> {
			Set<T> set = new LinkedHashSet<>(c.size());
			set.addAll(c);
			return Collections.unmodifiableSet(set);
		});
	}

	/**
	 * @param <T> The type of values in the input
	 * @param metric The metric whose values are the input
	 * @return An aggregation that returns the maximum value from the input
	 */
	public static <T extends Comparable<? super T>> MetricAggregation<? super T, T> max(MetricType<T> metric) {
		Function<Collection<? extends T>, T> fn = values -> {
			T max = null;
			for (T value : values) {
				if (value == null) {
					continue;
				} else if (max == null || value.compareTo(max) > 0) {
					max = value;
				}
			}
			if (max == null && metric.getType().unwrap().isPrimitive()) {
				max = primitiveNull(TypeTokens.get().unwrap(TypeTokens.getRawType(metric.getType())));
			}
			return max;
		};
		return new MetricAggregationImpl<>("MAX", metric.getType(), 1E-10, fn);
	}

	/**
	 * @param <T> The type of values in the input
	 * @param metric The metric whose values are the input
	 * @return An aggregation that returns the minimum value from the input
	 */
	public static <T extends Comparable<? super T>> MetricAggregation<? super T, T> min(MetricType<T> metric) {
		Function<Collection<? extends T>, T> fn = values -> {
			T min = null;
			for (T value : values) {
				if (value == null) {
					continue;
				} else if (min == null || value.compareTo(min) < 0) {
					min = value;
				}
			}
			if (min == null && metric.getType().unwrap().isPrimitive()) {
				min = primitiveNull(TypeTokens.get().unwrap(TypeTokens.getRawType(metric.getType())));
			}
			return min;
		};
		return new MetricAggregationImpl<>("MIN", metric.getType(), 1E-10, fn);
	}

	/**
	 * @param <T> The type of values in the input
	 * @param metric The metric whose values are the input
	 * @return An aggregation that returns the sum of the values from the input
	 */
	public static <T extends Number> MetricAggregation<T, ? extends Number> sum(MetricType<T> metric) {
		Function<Collection<? extends T>, Number> fn;
		TypeToken<? extends Number> type;
		Class<?> raw = TypeTokens.get().unwrap(TypeTokens.getRawType(metric.getType()));
		if (long.class.equals(raw) || int.class.equals(raw) || short.class.equals(raw) || byte.class.equals(raw)) {
			type = TypeTokens.get().LONG;
			fn = values -> {
				long sum = 0;
				for (T value : values) {
					sum += value.longValue();
				}
				return Long.valueOf(sum);
			};
		} else {
			type = TypeTokens.get().DOUBLE;
			fn = values -> {
				double sum = 0;
				for (T value : values) {
					sum += value.doubleValue();
				}
				return Double.valueOf(sum);
			};
		}

		return new MetricAggregationImpl<T, Number>("SUM", (TypeToken<Number>) type, 1E-10, fn);
	}

	/**
	 * @param <T> The type of values in the input
	 * @param metric The metric whose values are the input
	 * @return An aggregation that returns the average of the values from the input, or zero if no values were present in the input
	 */
	public static <T extends Number> MetricAggregation<T, Double> mean(MetricType<T> metric) {
		Function<Collection<? extends T>, Double> fn = values -> {
			double sum = 0;
			int count = 0;
			for (T value : values) {
				sum += value.doubleValue();
				count++;
			}
			return sum / count;
		};
		return new MetricAggregationImpl<T, Double>("MEAN", TypeTokens.get().DOUBLE, 1E-10, fn);
	}

	static <T> T primitiveNull(Class<?> type) {
		if (!type.isPrimitive()) {
			throw new IllegalArgumentException("Type " + type.getName() + " is not primitive");
		}
		if (double.class == type) {
			return (T) Double.valueOf(Double.NaN);
		} else if (float.class == type) {
			return (T) Float.valueOf(Float.NaN);
		} else if (long.class == type) {
			return (T) Long.valueOf(0);
		} else if (int.class == type) {
			return (T) Integer.valueOf(0);
		} else if (short.class == type) {
			return (T) Short.valueOf((short) 0);
		} else if (byte.class == type) {
			return (T) Byte.valueOf((byte) 0);
		} else if (char.class == type) {
			return (T) Character.valueOf((char) 0);
		} else if (boolean.class == type) {
			return (T) Boolean.FALSE;
		} else {
			throw new IllegalStateException("Unrecognized primitive type: " + type.getName());
		}
	}
}
