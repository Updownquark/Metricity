package org.metricity.metric.util;

import java.util.function.Consumer;

import org.metricity.anchor.Anchor;
import org.metricity.anchor.AnchorSource;
import org.metricity.anchor.AnchorType;
import org.metricity.metric.Metric;
import org.metricity.metric.MetricSupport;
import org.metricity.metric.SimpleMetricType;
import org.metricity.metric.service.MetricChannel;
import org.metricity.metric.service.MetricChannelService;

public interface MetricSatisfier<T> {
	SimpleMetricType<T> getMetric();

	String applies(AnchorSource anchorSource, AnchorType<?> anchorType);

	String isSatisfied(AnchorSource anchorSource, AnchorType<?> anchorType, MetricSupport support);

	MetricChannel<T> createChannel(Anchor anchor, Metric<T> metric, MetricChannelService depends, Consumer<String> onError);
}
