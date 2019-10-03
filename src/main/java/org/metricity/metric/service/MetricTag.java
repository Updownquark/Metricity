package org.metricity.metric.service;

import org.qommons.StringUtils;

/**
 * <p>
 * A metric tag is simply a category/label pair that describes some aspect of a {@link MetricChannel}.
 * </p>
 * <p>
 * I had a vision for this when I was implementing model switching (for propagation), but I went a different direction. I left this in,
 * since it's present in data channels and might aid in empirical/simulated metric continuity, but it's not currently used at all
 * </p>
 * 
 * @author abutler
 */
public interface MetricTag extends Comparable<MetricTag> {
	/** @return The category of the tag */
	String getCategory();

	/** @return The label (value) of the tag */
	String getLabel();

	@Override
	default int compareTo(MetricTag o) {
		int compare = StringUtils.compareNumberTolerant(getCategory(), o.getCategory(), true, true);
		if (compare == 0)
			compare = StringUtils.compareNumberTolerant(getLabel(), o.getLabel(), true, true);
		return compare;
	}
}
