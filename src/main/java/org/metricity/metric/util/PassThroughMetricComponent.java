package org.metricity.metric.util;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.metricity.anchor.Anchor;
import org.metricity.anchor.AnchorSource;
import org.metricity.anchor.AnchorType;
import org.metricity.metric.Metric;
import org.metricity.metric.MetricSupport;
import org.metricity.metric.SimpleMetricType;
import org.metricity.metric.service.MetricChannel;
import org.metricity.metric.service.MetricChannelService;
import org.metricity.metric.service.MetricChannelServiceComponent;
import org.metricity.metric.service.MetricQueryResult;
import org.metricity.metric.service.MetricTag;
import org.observe.Observable;

public interface PassThroughMetricComponent extends MetricChannelServiceComponent {
	MetricChannelServiceComponent getDelegate();

	@Override
	default double getCost(MetricChannel<?> channel) {
		return getDelegate().getCost(channel);
	}

	@Override
	default Set<MetricTag> getTags(MetricChannel<?> channel) {
		return getDelegate().getTags(channel);
	}

	@Override
	default boolean isConstant(MetricChannel<?> channel) {
		return getDelegate().isConstant(channel);
	}

	@Override
	default <T> MetricQueryResult<T> query(MetricChannel<T> channel, Consumer<Object> initialListener) {
		return getDelegate().query(channel, initialListener);
	}

	@Override
	default Collection<SimpleMetricType<?>> getAllSupportedMetrics(AnchorSource anchorSource, AnchorType<?> anchorType,
		MetricSupport dependencySupport) {
		return getDelegate().getAllSupportedMetrics(anchorSource, anchorType, dependencySupport);
	}

	@Override
	default Observable<?> supportChanges() {
		return getDelegate().supportChanges();
	}

	@Override
	default <T> List<? extends MetricChannel<T>> getChannels(Anchor anchor, Metric<T> metric, MetricChannelService dependencies,
		Consumer<String> onError) {
		return getDelegate().getChannels(anchor, metric, dependencies, onError);
	}

	@Override
	default <T> MetricChannel<T> getChannel(Anchor anchor, Metric<T> metric, MetricChannelService dependencies, Consumer<String> onError) {
		return getDelegate().getChannel(anchor, metric, dependencies, onError);
	}
}
