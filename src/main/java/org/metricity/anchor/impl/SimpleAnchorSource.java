package org.metricity.anchor.impl;

import org.metricity.anchor.AnchorSource;
import org.metricity.metric.util.MetricUtils;
import org.qommons.Named;

public class SimpleAnchorSource extends Named.AbstractNamed implements AnchorSource {
	private int hashCode = -1;

	public SimpleAnchorSource(String name) {
		super(name);
	}

	@Override
	public int hashCode() {
		if (hashCode == -1)
			hashCode = MetricUtils.hashName(getName());
		return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		return this == obj;
	}

	@Override
	public String toString() {
		return getName();
	}
}
