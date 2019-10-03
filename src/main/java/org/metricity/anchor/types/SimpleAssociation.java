package org.metricity.anchor.types;

import java.util.Objects;

import org.metricity.anchor.Anchor;
import org.metricity.anchor.AnchorType;

public class SimpleAssociation<A extends Anchor> implements AssociationAnchor<A> {
	private final AnchorType<A> theAnchorType;
	private final A theSender;
	private final A theReceiver;
	private final int hashCode;

	public SimpleAssociation(AnchorType<A> anchorType, A sourceAnchor, A destAnchor) {
		if (!sourceAnchor.getWorld().equals(destAnchor.getWorld())) {
			throw new IllegalArgumentException("Source and destination anchors must exist in the same world");
		}
		theAnchorType = anchorType;
		theSender = sourceAnchor;
		theReceiver = destAnchor;
		hashCode = Objects.hash(theSender, theReceiver);
	}

	@Override
	public AnchorType<A> getType() {
		return theAnchorType;
	}

	@Override
	public A getSourceAnchor() {
		return theSender;
	}

	@Override
	public A getDestAnchor() {
		return theReceiver;
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (!(obj instanceof AssociationAnchor) || hashCode != obj.hashCode()) {
			return false;
		} else {
			return theSender.equals(((AssociationAnchor<?>) obj).getSourceAnchor())
				&& theReceiver.equals(((AssociationAnchor<?>) obj).getDestAnchor());
		}
	}

	@Override
	public String toString() {
		return getName();
	}
}
