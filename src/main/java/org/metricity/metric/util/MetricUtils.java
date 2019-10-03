package org.metricity.metric.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.metricity.anchor.Anchor;
import org.metricity.metric.MetricParameter;
import org.metricity.metric.MetricType;
import org.metricity.metric.MetricTypeBuilder;
import org.metricity.metric.MultiRelationMetricType;
import org.metricity.metric.SimpleMetric;
import org.metricity.metric.util.derived.MetricDependencyType;
import org.observe.util.TypeTokens;
import org.qommons.QommonsUtils;

import com.google.common.reflect.TypeToken;

public class MetricUtils {
	private static final List<Boolean> ALL_BOOLEANS = Collections.unmodifiableList(Arrays.asList(Boolean.FALSE, Boolean.TRUE));

	public static <T> List<T> getParamEnumValues(MetricParameter<T> param) {
		if (!param.getAllowedValues().isEmpty()) {
			return param.getAllowedValues();
		} else if (TypeTokens.get().isBoolean(param.getType())) {
			return (List<T>) ALL_BOOLEANS; // Let's this parameter won't filter out a boolean, shall we?
		} else {
			return null;
		}
	}

	private static final boolean USE_FNV = true;
	private static final int FNV_OFFSET_BASIS = 0x811c9dc5;
	private static final int FNV_PRIME = 16777619;

	public static int hashName(String name) {
		if (USE_FNV) {
			int hash = FNV_OFFSET_BASIS;
			for (int c = 0; c < name.length(); c++) {
				hash = (hash * FNV_PRIME) ^ name.charAt(c);
			}
			return hash;
		} else {
			return name.hashCode();
		}
	}

	public static <A extends Anchor, C extends Collection<? extends A>> //
	MetricDependencyType<C, Collection<AnchorExists<A>>> buildExistingMembers(
			MultiRelationMetricType<A, ? extends List<? extends A>> allMembersMetric, MetricDependencyType.AnchorDefiner<?, C> dep) {
		return dep.self()
				.metric(allMembersMetric.aggregate((MetricType<AnchorExists<A>>) (MetricType<?>) ANCHOR_EXISTS.getType()).build().build())
				.build();
	}

	public static <A extends Anchor> List<A> unwrapAnchorExists(Collection<AnchorExists<A>> anchorExists) {
		return QommonsUtils.filterMap(anchorExists, ax -> ax.exists, ax -> ax.anchor);
	}

	public static class AnchorExists<A extends Anchor> {
		public final A anchor;
		public final boolean exists;

		public AnchorExists(A anchor, boolean exists) {
			this.anchor = anchor;
			this.exists = exists;
		}

		@Override
		public int hashCode() {
			return anchor.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			} else if (!(obj instanceof AnchorExists)) {
				return false;
			}
			AnchorExists<?> ax = (AnchorExists<?>) obj;
			return exists == ax.exists && anchor.equals(ax.anchor);
		}

		@Override
		public String toString() {
			return anchor + " (" + (exists ? "exists" : "absent") + ")";
		}
	}

	public static final SimpleMetric<AnchorExists<?>> ANCHOR_EXISTS = MetricTypeBuilder
			.build("Anchor Exists", new TypeToken<AnchorExists<?>>() {
			}).internalOnly().build().build().build();
}
