package org.metricity.metric.service;

import java.util.Map;

import org.metricity.anchor.Anchor;
import org.metricity.anchor.AnchorType;
import org.observe.collect.ObservableCollection;

/** A component that may inject anchors into the current state in a {@link CurrentAnchorStateService} */
public interface CurrentAnchorSetComponent {
	/**
	 * @param anchorStates
	 *            The anchor state service
	 * @return The collections of anchors (by type name) that this component is to supply
	 */
	Map<AnchorType<?>, ? extends ObservableCollection<? extends Anchor>> getAnchors(CurrentAnchorStateService anchorStates);
}
