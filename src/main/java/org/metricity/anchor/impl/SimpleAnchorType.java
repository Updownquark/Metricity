package org.metricity.anchor.impl;

import org.metricity.anchor.Anchor;
import org.metricity.anchor.AnchorType;
import org.metricity.metric.util.MetricUtils;
import org.qommons.Named;

import com.google.common.reflect.TypeToken;

public class SimpleAnchorType<A extends Anchor> extends Named.AbstractNamed implements AnchorType<A> {
	private final TypeToken<A> theType;
	private int hashCode = -1;

	public SimpleAnchorType(TypeToken<A> type, String name) {
		super(name);
		theType = type;
	}

	@Override
	public TypeToken<A> getType() {
		return theType;
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
