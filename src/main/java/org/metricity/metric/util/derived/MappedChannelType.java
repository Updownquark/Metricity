package org.metricity.metric.util.derived;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.metricity.anchor.Anchor;
import org.metricity.anchor.AnchorSource;
import org.metricity.anchor.AnchorType;
import org.metricity.metric.Metric;
import org.metricity.metric.MetricSupport;
import org.metricity.metric.SimpleMetric;
import org.metricity.metric.SimpleMetricType;
import org.metricity.metric.service.MetricChannel;
import org.metricity.metric.service.MetricChannelService;
import org.metricity.metric.service.MetricTag;

public class MappedChannelType<X, T> extends AbstractDerivedChannelType<T> {
	private final MetricDependencyType<T, X> theDependency;
	private final Function<TransformSource<Anchor, X, T>, T> theMap;

	public MappedChannelType(SimpleMetricType<T> metric, //
		Predicate<AnchorSource> anchorSource, Predicate<AnchorType<?>> anchorType, Function<Anchor, String> anchorFilter, //
		Function<? super SimpleMetric<T>, String> metricFilter, Set<MetricTag> tags, //
		MetricDependencyType<T, X> dependency, Function<TransformSource<Anchor, X, T>, T> map, double calculationCost) {
		super(metric, anchorSource, anchorType, anchorFilter, metricFilter, tags, calculationCost);
		theDependency = dependency;
		theMap = map;
	}

	protected Function<TransformSource<Anchor, X, T>, T> getValue() {
		return theMap;
	}

	public MetricDependencyType<T, X> getDependency() {
		return theDependency;
	}

	@Override
	public String isSatisfied(AnchorSource anchorSource, AnchorType<?> anchorType, MetricSupport support) {
		String err = applies(anchorSource, anchorType);
		if (err == null && support == null) {
			return null; // This method may be called when support is unknown to just test against the anchor
		}
		if (err == null) {
			err = support.isSupported(anchorSource, theDependency.getAnchorType(anchorType), theDependency.getMetric());
		}
		return err;
	}

	@Override
	public MetricChannel<T> createChannel(Anchor anchor, Metric<T> metric, MetricChannelService depends, Consumer<String> onError) {
		String err = applies(anchor, metric);
		if (err != null) {
			if (onError != null) {
				onError.accept(err);
			}
			return null;
		}
		Anchor sourceAnchor = theDependency.getAnchor(anchor, metric);
		Metric<X> sourceMetric = theDependency.getMetric(anchor, metric);
		MetricChannel<X> sourceChannel;
		if (theDependency.hasChannelFilter()) {
			DependencyInstanceSet<Anchor, T> ds = new DependencyInstanceSet<Anchor, T>() {
				@Override
				public boolean isSupported(String dependencyName) {
					throw new UnsupportedOperationException();
				}

				@Override
				public Anchor getAnchor() {
					return anchor;
				}

				@Override
				public Metric<T> getMetric() {
					return metric;
				}
			};
			sourceChannel = theDependency.filterChannels(depends.getChannels(sourceAnchor, sourceMetric, onError), ds, depends);
		} else {
			sourceChannel = depends.getChannel(sourceAnchor, sourceMetric, onError);
		}
		if (sourceChannel == null) {
			return null;
		}
		return new MappedChannel<>(this, anchor, metric, sourceChannel);
	}

	public T map(Anchor anchor, Metric<T> metric, MetricChannel<X> sourceChannel, X srcValue) {
		if (theMap == null) {
			return (T) srcValue;
		}
		TransformSource<Anchor, X, T> ts = new TransformSource<Anchor, X, T>() {
			@Override
			public Anchor getAnchor() {
				return anchor;
			}

			@Override
			public Metric<T> getMetric() {
				return metric;
			}

			@Override
			public MetricChannel<X> getChannel() {
				return sourceChannel;
			}

			@Override
			public X get() {
				return srcValue;
			}
		};
		return theMap.apply(ts);
	}
}
