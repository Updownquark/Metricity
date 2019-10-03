package org.metricity.anchor.types;

import org.metricity.anchor.Anchor;

public interface AssociationAnchor<A extends Anchor> extends Anchor {
	@Override
	default WorldAnchor getWorld() {
		return getSourceAnchor().getWorld();
	}

	@Override
	default String getName() {
		return getSourceAnchor() + "->" + getDestAnchor();
	}

	@Override
	default int compareTo(Anchor o) {
		int comp = Anchor.compareSourceAndType(this, o);
		if (comp == 0) {
			AssociationAnchor<?> other = (AssociationAnchor<?>) o;
			comp = getSourceAnchor().compareTo(other.getSourceAnchor());
			if (comp == 0) {
				comp = getSourceAnchor().compareTo(other.getDestAnchor());
			}
		}
		return comp;
	}

	A getSourceAnchor();
	A getDestAnchor();
}
