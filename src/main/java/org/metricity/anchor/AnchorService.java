package org.metricity.anchor;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;

import org.metricity.anchor.types.WorldAnchor;
import org.observe.Subscription;
import org.qommons.Causable;


public interface AnchorService {
	public enum ChangeType {
		ADD, REMOVE;
	}

	class AnchorChangeEvent extends Causable {
		private final ChangeType theChangeType;
		private final Collection<Anchor> theAnchors;
		private final Function<Anchor, Anchor> theCopySources;
		private final Function<Anchor, Anchor> theSourceCopies;

		public AnchorChangeEvent(ChangeType changeType, Collection<Anchor> anchors, Function<Anchor, Anchor> copySources,
				Function<Anchor, Anchor> sourceCopies, Object cause) {
			super(cause);
			theChangeType = changeType;
			theAnchors = anchors;
			theCopySources = copySources;
			theSourceCopies = sourceCopies;
		}

		public ChangeType getChangeType() {
			return theChangeType;
		}

		public Collection<Anchor> getAnchors() {
			return theAnchors;
		}

		/**
		 * This method is only for events that represent a scenario copy operation, and will always return null otherwise
		 * 
		 * @param copiedAnchor
		 *            The anchor in the new world
		 * @return The anchor in the world that was copied that is the source of the given anchor, or null if the new anchor did not have a
		 *         source
		 */
		public Anchor getCopySource(Anchor copiedAnchor) {
			return theCopySources == null ? null : theCopySources.apply(copiedAnchor);
		}

		/**
		 * This method is only for events that represent a scenario copy operation, and will always return null otherwise
		 * 
		 * @param copySource
		 *            The anchor from the world that was copied
		 * @return The anchor in the new world that is the copy of the source anchor, or null if the given anchor was not copied
		 */
		public Anchor getCopy(Anchor copySource) {
			return theSourceCopies == null ? null : theSourceCopies.apply(copySource);
		}
	}

	Collection<? extends WorldAnchor> getKnownWorlds();
	WorldAnchor getWorld(AnchorSource source, String persisted);

	<A extends Anchor> Collection<A> getAnchors(WorldAnchor world, AnchorType<A> anchorType);
	String persist(Anchor anchor);
	<A extends Anchor> A getAnchor(WorldAnchor world, AnchorType<A> anchorType, String persisted);

	Subscription addAnchorListener(Consumer<AnchorChangeEvent> listener);
}
