package org.metricity.anchor.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.metricity.anchor.Anchor;
import org.metricity.anchor.AnchorType;
import org.observe.Subscription;
import org.observe.collect.CollectionChangeType;
import org.observe.collect.ObservableCollection;
import org.observe.collect.ObservableSet;
import org.observe.collect.ObservableSortedSet;
import org.observe.util.TypeTokens;
import org.qommons.Transaction;
import org.qommons.collect.BetterHashMap;
import org.qommons.collect.BetterSortedMap;
import org.qommons.collect.ElementId;
import org.qommons.collect.MapEntryHandle;
import org.qommons.collect.MutableCollectionElement.StdMsg;
import org.qommons.tree.BetterTreeMap;

import com.google.common.reflect.TypeToken;

public class ComponentAnchorAssembly<C, D> {
	static class TypedAnchors<A extends Anchor> {
		final AnchorType<A> anchorType;
		final ObservableCollection<? extends ObservableCollection<A>> anchorsBySource;
		final ObservableSortedSet<A> anchors;

		TypedAnchors(AnchorType<A> anchorType) {
			this.anchorType = anchorType;
			ObservableCollection<ObservableCollection<A>> bySource = ObservableCollection.create(//
				ObservableCollection.TYPE_KEY.getCompoundType(anchorType.getType()));
			anchorsBySource = bySource;
			anchors = bySource.flow().flatMap(anchorType.getType(), c -> c.flow()).distinctSorted(Anchor::compareTo, false).unmodifiable()
					.collect();
		}
	}

	public interface AnchorProducer {
		<A extends Anchor> ObservableCollection<A> getAnchors(AnchorType<A> anchorType);
	}

	static class DynamicAnchorComponent {
		private final ObservableSet<AnchorType<?>> theKnownAnchorTypes;
		private final AnchorProducer theAnchorProducer;
		private final Map<AnchorType<?>, ElementId> theAnchorRefs; // For de-registering
		private Subscription anchorSub;

		DynamicAnchorComponent(ObservableSet<AnchorType<?>> knownAnchorTypes, AnchorProducer anchorProducer) {
			theKnownAnchorTypes = knownAnchorTypes;
			theAnchorProducer = anchorProducer;
			theAnchorRefs = new HashMap<>();
		}
	}

	private final ObservableSortedSet<AnchorType<?>> theKnownAnchorTypes;
	private final BetterSortedMap<AnchorType<?>, TypedAnchors<?>> theTypedAnchors;
	private final ObservableCollection<ObservableCollection<? extends Anchor>> theTypedAnchorCollection;
	private final ObservableCollection<Anchor> theAnchors;
	private final Map<C, Map<AnchorType<?>, ElementId>> theComponentAnchorRefs; // For de-registering
	private final Map<D, DynamicAnchorComponent> theDynamicComponents;

	public ComponentAnchorAssembly() {
		theTypedAnchors = new BetterTreeMap<>(true, AnchorType::compareTo);
		theTypedAnchorCollection = ObservableCollection.create(new TypeToken<ObservableCollection<? extends Anchor>>() {
		});
		theAnchors = theTypedAnchorCollection.flow().flatMap(TypeTokens.get().of(Anchor.class), c -> c.flow()).filterMod(//
				f -> f.unmodifiable(StdMsg.UNSUPPORTED_OPERATION, false)).collect();
		theComponentAnchorRefs = BetterHashMap.build().identity().buildMap();
		theDynamicComponents = BetterHashMap.build().identity().buildMap();
		theKnownAnchorTypes = ObservableSortedSet.create(AnchorType.TYPE, AnchorType::compareTo);

		theKnownAnchorTypes.onChange(evt -> {
			if (evt.getType() != CollectionChangeType.add) {
				return;
			}

			anchorTypeAdded(evt.getNewValue());
		});
	}

	<A extends Anchor> void anchorTypeAdded(AnchorType<A> type) {
		TypedAnchors<A> typedAnchors;
		typedAnchors = new TypedAnchors<>(type);
		// Add anchors from dynamic components
		for (DynamicAnchorComponent dac : theDynamicComponents.values()) {
			ObservableCollection<A> anchors = dac.theAnchorProducer.getAnchors(type);
			ElementId id = ((ObservableCollection<ObservableCollection<? extends Anchor>>) typedAnchors.anchorsBySource)//
				.addElement(anchors, false).getElementId();
			dac.theAnchorRefs.put(type, id);
		}

		MapEntryHandle<AnchorType<?>, TypedAnchors<?>> entry = theTypedAnchors.putEntry(type, typedAnchors, false);
		// Add the new type's anchors into the overall anchor collection in the type's order
		int index = theTypedAnchors.keySet().getElementsBefore(entry.getElementId());
		theTypedAnchorCollection.add(index, entry.get().anchors);
	}

	public void registerComponent(C component, Map<AnchorType<?>, ? extends ObservableCollection<? extends Anchor>> componentAnchors) {
		Map<AnchorType<?>, ElementId> componentAnchorRefs = new HashMap<>();
		theComponentAnchorRefs.put(component, componentAnchorRefs);
		try (Transaction t = theTypedAnchors.lock(true, null)) {
			for (Map.Entry<AnchorType<?>, ? extends ObservableCollection<? extends Anchor>> anchorTypeEntry : componentAnchors.entrySet()) {
				theKnownAnchorTypes.add(anchorTypeEntry.getKey());
				TypedAnchors<?> typedAnchors = theTypedAnchors.get(anchorTypeEntry.getKey());
				ElementId id = ((ObservableCollection<ObservableCollection<? extends Anchor>>) typedAnchors.anchorsBySource)//
						.addElement(anchorTypeEntry.getValue(), false).getElementId();
				componentAnchorRefs.put(anchorTypeEntry.getKey(), id);
			}
		}
	}

	public void registerDynamicComponent(D component, ObservableSet<AnchorType<?>> anchorTypes, AnchorProducer anchors) {
		DynamicAnchorComponent dac = new DynamicAnchorComponent(anchorTypes, anchors);
		try (Transaction kat = theKnownAnchorTypes.lock(false, null)) {
			theDynamicComponents.put(component, dac);
			try (Transaction t = theTypedAnchors.lock(false, null)) {
				for (Entry<AnchorType<?>, TypedAnchors<?>> entry : theTypedAnchors.entrySet()) {
					ElementId id = ((ObservableCollection<ObservableCollection<? extends Anchor>>) entry.getValue().anchorsBySource)//
						.addElement(anchors.getAnchors(entry.getKey()), false).getElementId();
					dac.theAnchorRefs.put(entry.getKey(), id);
				}
			}
		}
		try (Transaction t = anchorTypes.lock(false, null)) {
			theKnownAnchorTypes.addAll(anchorTypes);
			dac.anchorSub = anchorTypes.changes().act(evt -> {
				if (evt.type == CollectionChangeType.add) {
					theKnownAnchorTypes.addAll(evt.getValues());
				}
			});
		}
	}

	// TODO removeComponent methods

	public ObservableSortedSet<AnchorType<?>> getKnownAnchorTypes() {
		return theKnownAnchorTypes.flow().unmodifiable().collectPassive();
	}

	public ObservableCollection<Anchor> getAllAnchors() {
		return theAnchors;
	}

	public <A extends Anchor> ObservableSortedSet<A> getAnchors(AnchorType<A> type) {
		theKnownAnchorTypes.add(type);
		return (ObservableSortedSet<A>) theTypedAnchors.get(type).anchors;
	}
}