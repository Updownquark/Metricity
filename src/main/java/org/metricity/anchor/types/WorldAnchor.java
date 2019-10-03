package org.metricity.anchor.types;

import org.metricity.anchor.Anchor;
import org.metricity.anchor.AnchorSource;
import org.metricity.anchor.AnchorType;
import org.metricity.anchor.impl.SimpleAnchorType;
import org.observe.util.TypeTokens;

public interface WorldAnchor extends Anchor {
	public static final AnchorType<WorldAnchor> ANCHOR_TYPE = new SimpleAnchorType<>(TypeTokens.get().of(WorldAnchor.class), "WORLD");

	@Override
	default WorldAnchor getWorld() {
		return this;
	}

	@Override
	abstract AnchorSource getSource();

	@Override
	default AnchorType<WorldAnchor> getType() {
		return ANCHOR_TYPE;
	}

	@Override
	default String getName() {
		return getSource().getName();
	}

	public static WorldAnchor createWorld(AnchorSource source) {
		return new WorldAnchor() {
			@Override
			public AnchorSource getSource() {
				return source;
			}

			@Override
			public int compareTo(Anchor o) {
				return Anchor.compareSourceAndType(this, o);
			}
		};
	}
}
