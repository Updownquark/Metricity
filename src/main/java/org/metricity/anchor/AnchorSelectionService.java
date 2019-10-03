package org.metricity.anchor;

import org.metricity.anchor.types.WorldAnchor;
import org.observe.ObservableValue;
import org.observe.collect.ObservableCollection;
import org.observe.collect.ObservableSet;

public interface AnchorSelectionService {
	ObservableValue<WorldAnchor> getSelectedWorld();

	ObservableSet<AnchorType<?>> getAllKnownAnchorTypes();

	ObservableCollection<Anchor> getSelectedAnchors();

	<A extends Anchor> ObservableCollection<A> getSelectedAnchors(AnchorType<A> type);
}
