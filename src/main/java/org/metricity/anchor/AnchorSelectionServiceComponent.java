package org.metricity.anchor;

import java.util.Map;

import org.observe.collect.ObservableCollection;

public interface AnchorSelectionServiceComponent {
	Map<String, ObservableCollection<? extends Anchor>> getAnchors(AnchorSelectionService rootService);
}
