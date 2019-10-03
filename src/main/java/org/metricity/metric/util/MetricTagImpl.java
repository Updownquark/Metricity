package org.metricity.metric.util;

import java.util.Objects;

import org.metricity.metric.service.MetricTag;

public class MetricTagImpl implements MetricTag {
	private final String theCategory;
	private final String theLabel;

	public MetricTagImpl(String category, String label) {
		theCategory = category;
		theLabel = label;
	}

	@Override
	public String getCategory() {
		return theCategory;
	}

	@Override
	public String getLabel() {
		return theLabel;
	}

	@Override
	public int hashCode() {
		return Objects.hash(theCategory, theLabel);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (!(obj instanceof MetricTag)) {
			return false;
		}
		MetricTag other = (MetricTag) obj;
		return theCategory.equals(other.getCategory()) && theLabel.equals(other.getLabel());
	}

	@Override
	public String toString() {
		return new StringBuilder(theCategory).append('/').append(theLabel).toString();
	}
}