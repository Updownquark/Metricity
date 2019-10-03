package org.metricity.metric.util;

import java.util.function.Consumer;
import java.util.function.Function;

import org.metricity.metric.service.MetricChannel;
import org.metricity.metric.service.MetricQueryOptions;
import org.metricity.metric.service.MetricQueryResult;
import org.metricity.metric.service.MetricResult;
import org.observe.Subscription;
import org.qommons.Identifiable;
import org.qommons.Transaction;

import com.google.common.reflect.TypeToken;

public class MetricTimelineWrapper<X, T> implements MetricQueryResult<T> {
	private final MetricChannel<T> theChannel;
	private final MetricQueryResult<X> theSourceResults;
	private final Function<? super X, ? extends T> theMap;
	private TypeToken<MetricResult<T>> theType;
	private Object theIdentity;

	MetricTimelineWrapper(MetricChannel<T> channel, MetricQueryResult<X> results, Function<? super X, ? extends T> map) {
		theChannel = channel;
		theSourceResults = results;
		theMap = map;
	}

	protected MetricQueryResult<X> getSourceTimeline() {
		return theSourceResults;
	}

	@Override
	public MetricChannel<T> getChannel() {
		return theChannel;
	}

	@Override
	public TypeToken<MetricResult<T>> getType() {
		if (theType == null)
			theType = MetricResult.TYPE_KEY.getCompoundType(theChannel.getMetric().getType().getType());
		return theType;
	}

	@Override
	public Transaction lock() {
		return theSourceResults.lock();
	}

	@Override
	public Transaction tryLock() {
		return theSourceResults.tryLock();
	}

	@Override
	public long getStamp() {
		return theSourceResults.getStamp();
	}

	@Override
	public Object getIdentity() {
		if (theIdentity == null)
			theIdentity = Identifiable.wrap(theSourceResults.getIdentity(), theMap.toString());
		return theIdentity;
	}

	@Override
	public MetricResult<T> getResult(MetricResult<T> result, MetricQueryOptions options) {
		theSourceResults.getResult((MetricResult<X>) result, options);
		if (theMap != null && result.isAvailable())
			result.setValue(theMap.apply((X) result.get()));
		return result;
	}

	@Override
	public Subscription notifyOnChange(Consumer<Object> onChange) {
		return theSourceResults.notifyOnChange(onChange);
	}

	protected T map(X value) {
		if (value == null || theMap == null) {
			return (T) value;
		}
		return theMap.apply(value);
	}

	@Override
	public void checkDynamicDependencies() {
		theSourceResults.checkDynamicDependencies();
	}

	@Override
	public void unsubscribe() {
		theSourceResults.unsubscribe();
	}

	@Override
	public boolean isValid() {
		return theSourceResults.isValid();
	}

	@Override
	public String toString() {
		return getIdentity().toString();
	}

	public static <X, T> MetricTimelineWrapper<X, T> wrap(MetricChannel<T> channel, MetricQueryResult<X> srcResults,
		Function<? super X, ? extends T> map) {
		return new MetricTimelineWrapper<>(channel, srcResults, map);
	}
}
