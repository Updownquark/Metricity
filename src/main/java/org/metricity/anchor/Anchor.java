package org.metricity.anchor;

import org.metricity.anchor.types.WorldAnchor;
import org.qommons.Named;

/**
 * An item/entity/thing that can be reasoned on. An anchor offers very little information on what it actually represents, through this
 * interface.
 */
public interface Anchor extends Named, Comparable<Anchor> {
	/** @return The world in which this anchor exists */
	WorldAnchor getWorld();

	/** @return A unique identifier of the data source that manages this anchor */
	default AnchorSource getSource() {
		return getWorld().getSource();
	}

	/** @return The type of this anchor. */
	AnchorType<?> getType();

	/**
	 * <p>
	 * A human-readable representation of this anchor. May be modifiable.
	 * </p>
	 * <p>
	 * Must be unique within its {@link #getWorld() world}. That is, for all anchors a1 and a2 for which
	 * <code>a1.getName().equals(a2.getName())</code>, it is also true that <code>a1.equals(a2)</code>.
	 * </p>
	 * <p>
	 * Anchors that are copies of each other should share the same name immediately after the copy operation, with the exception of
	 * {@link WorldAnchor world}s. Therefore, this string should not contain any globally-unique information such as database IDs.
	 * </p>
	 * 
	 * @return A human-readable representation of this anchor
	 */
	@Override
	String getName();

	public static int compareSourceAndType(Anchor anchor1, Anchor anchor2) {
		int comp = anchor1.getType().compareTo(anchor2.getType());
		if (comp == 0)
			comp = anchor1.getSource().compareTo(anchor2.getSource());
		return comp;
	}
}
