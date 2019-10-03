package org.metricity.metric.service.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.dbug.DBug;
import org.dbug.DBugAnchor;
import org.dbug.DBugAnchorType;
import org.metricity.anchor.Anchor;
import org.metricity.anchor.AnchorSelectionService;
import org.metricity.anchor.AnchorType;
import org.metricity.anchor.service.impl.ComponentAnchorAssembly;
import org.metricity.metric.AnchorMetric;
import org.metricity.metric.Metric;
import org.metricity.metric.StandardMetrics;
import org.metricity.metric.service.CurrentAnchorSetComponent;
import org.metricity.metric.service.CurrentAnchorStateService;
import org.metricity.metric.service.MetricChannel;
import org.metricity.metric.service.MetricChannelService;
import org.metricity.metric.service.MetricQueryOptions;
import org.metricity.metric.service.MetricQueryResult;
import org.metricity.metric.service.MetricResult;
import org.metricity.metric.service.MetricWork;
import org.metricity.metric.service.MetricWorkImpl;
import org.observe.Observable;
import org.observe.SimpleObservable;
import org.observe.Subscription;
import org.observe.collect.CollectionChangeEvent;
import org.observe.collect.CollectionChangeType;
import org.observe.collect.ObservableCollection;
import org.observe.collect.ObservableCollection.DistinctSortedDataFlow;
import org.observe.collect.ObservableSortedSet;
import org.observe.util.TypeTokens;
import org.qommons.BiTuple;
import org.qommons.Causable;
import org.qommons.QommonsUtils;
import org.qommons.Transaction;
import org.qommons.ValueHolder;
import org.qommons.collect.BetterHashMap;
import org.qommons.collect.BetterSortedSet.SortedSearchFilter;
import org.qommons.collect.CollectionElement;
import org.qommons.collect.ListenerList;
import org.qommons.collect.MapEntryHandle;
import org.qommons.collect.MutableCollectionElement.StdMsg;
import org.qommons.collect.MutableMapEntryHandle;
import org.qommons.collect.StampedLockingStrategy;
import org.qommons.debug.Debug;

import com.google.common.reflect.TypeToken;

public class CurrentAnchorStateServiceImpl implements CurrentAnchorStateService {
	/** Debugging controls. Using these to debug the code; should always be false in the repo. */
	private static boolean DEBUG_CHANNELS = false;
	private static boolean DEBUG_ANCHORS = false;

	private static boolean REUSE_TIMELINES = true;

	private AnchorSelectionService theAnchorService;
	private MetricChannelService theMetricService;

	private ObservableSortedSet<Anchor> theAnchors;
	private final Map<AnchorType<?>, ObservableSortedSet<? extends Anchor>> theTypedAnchors;
	private final ComponentAnchorAssembly<CurrentAnchorSetComponent, AnchorSelectionService> theAssembly;
	private List<CurrentAnchorSetComponent> thePreLoadedComponents;

	private final ConcurrentHashMap<BiTuple<Anchor, Metric<?>>, WatchedChannel<?>> theWatchedChannels;
	private final ConcurrentLinkedQueue<WatchedChannel<?>> theChannelsToCheck;
	private final ListenerList<ChannelChecker> theWaitingCheckers;
	private final ChannelCheckerThread[] theChannelCheckers;
	private final DebugChannelCheckerThread theDebugChecker;
	private final AtomicInteger isParallelChecking;
	private final ValueStateImpl theExistenceWatcher;
	private final SimpleObservable<Object> theExistenceUpdate;
	private final AtomicBoolean isExistenceChanged;

	private final ObservableCollection<MetricChannel<Boolean>> theScenarioAffectingChannels;

	private final ListenerList<ValueStateImpl> theWatchers;
	private long theCurrentUpdate;

	private final ObservableSortedSet<Anchor> theSelectedAnchorsCopy;
	private final ConcurrentLinkedQueue<CollectionChangeEvent<Anchor>> theSelectedAnchorChanges;

	private volatile boolean supportChanged;
	private final AtomicBoolean environmentChanged;
	// These are package-private to avoid unnecessary synthetic accessor methods
	final MetricWorkImpl theWork;

	public CurrentAnchorStateServiceImpl() {
		theTypedAnchors = new HashMap<>();
		theWatchedChannels = new ConcurrentHashMap<>();
		theChannelsToCheck = new ConcurrentLinkedQueue<>();
		theWaitingCheckers = ListenerList.build().forEachSafe(false).build();
		isParallelChecking = new AtomicInteger();
		int parallelism = Runtime.getRuntime().availableProcessors();
		theChannelCheckers = new ChannelCheckerThread[parallelism + (DEBUG_CHANNELS ? 1 : 0)];
		ThreadGroup channelCheckerGroup = new ThreadGroup("Current Metric Updates");
		for (int i = 0; i < parallelism; i++) {
			theChannelCheckers[i] = new ChannelCheckerThread(channelCheckerGroup, i, false);
			theChannelCheckers[i].start();
		}
		if (DEBUG_CHANNELS) {
			theDebugChecker = new DebugChannelCheckerThread(channelCheckerGroup);
			theChannelCheckers[theChannelCheckers.length - 1] = theDebugChecker;
			theDebugChecker.start();
		} else {
			theDebugChecker = null;
		}
		theWatchers = new ListenerList<>("Should not happen");

		theSelectedAnchorsCopy = ObservableSortedSet.create(TypeTokens.get().of(Anchor.class), Anchor::compareTo);
		theSelectedAnchorChanges = new ConcurrentLinkedQueue<>();

		theAssembly = new ComponentAnchorAssembly<>();
		thePreLoadedComponents = new LinkedList<>();

		theScenarioAffectingChannels = ObservableCollection.create(new TypeToken<MetricChannel<Boolean>>() {});

		theWork = new MetricWorkImpl(true, () -> {
			List<MetricChannel<?>> snapshot = new ArrayList<>(theChannelCheckers.length);
			for (int i = 0; i < theChannelCheckers.length; i++) {
				MetricChannel<?> job = theChannelCheckers[i].theCurrentJob;
				if (job != null) {
					snapshot.add(job);
				}
			}
			return snapshot.stream();
		});

		theExistenceWatcher = new ValueStateImpl();
		theExistenceWatcher.withNotifyLevel(NotificationLevel.COMPLETE_EACH);
		theExistenceWatcher.watchMetric(StandardMetrics.EXISTS);
		theExistenceUpdate = new SimpleObservable<>();
		isExistenceChanged = new AtomicBoolean();
		theExistenceWatcher.onChange(evt -> {
			if (!isExistenceChanged.getAndSet(true)) {
				theWork.workQueued(null, EXISTENCE_CHANGE_COST);
			}
		});

		environmentChanged = new AtomicBoolean();
	}

	protected void setAnchorService(AnchorSelectionService anchorService) {
		theAnchorService = anchorService;
		theAssembly.registerDynamicComponent(theAnchorService, theAnchorService.getAllKnownAnchorTypes(),
			anchorService::getSelectedAnchors);
	}

	protected void setMetricService(MetricChannelService metricService) {
		theMetricService = metricService;
	}

	protected void registerComponent(CurrentAnchorSetComponent component) {
		// This method may be called during dependency satisfaction, before activate has been called,
		// but we can't actually do the registration until the anchor collection has been set up
		boolean registerNow;
		synchronized (theAssembly) {
			registerNow = thePreLoadedComponents == null;
			if (!registerNow) {
				thePreLoadedComponents.add(component);
			}
		}
		if (registerNow) {
			theAssembly.registerComponent(component, component.getAnchors(this));
		}
	}

	protected void activate() {
		if (DEBUG_ANCHORS || DEBUG_CHANNELS) {
			StampedLockingStrategy.STORE_WRITERS = true;
			Debug.d().start();
		}
		// Set up metrics

		// Set up anchors and scenario duration
		try (Transaction t = theAssembly.getAllAnchors().lock(false, null)) {
			theSelectedAnchorsCopy.addAll(theAssembly.getAllAnchors());
			if (DEBUG_ANCHORS) {
				System.out.println("ini: " + theSelectedAnchorsCopy);
			}
			// Maintain our own set of anchors asynchronously, only modifying it from the update thread to avoid watch lock contention
			theAssembly.getAllAnchors().changes().act(evt -> {
				// Assume that updates, if they even happen, are just updates, not replacements
				if (evt.type != CollectionChangeType.set) {
					if (DEBUG_ANCHORS) {
						System.out.println("src: " + evt);
					}
					envChange();
					theSelectedAnchorChanges.add(evt);
				}
			});
		}
		// Populate the channels that govern the scenario duration from all selected anchors
		// Don't use just the existing anchors, because existence may change with time
		// We need to keep a copy if the anchors whose duration is being watched, because the selectedAnchorsCopy may be out of sync
		// with it from time to time due to batching
		for (Anchor a : theSelectedAnchorsCopy) {
			theScenarioAffectingChannels.add(theMetricService.getChannel(a, StandardMetrics.OF_INTEREST, null));
		}
		theSelectedAnchorsCopy.changes().act(evt -> {
			switch (evt.type) {
			case add:
				try (Transaction sct = theScenarioAffectingChannels.lock(true, evt)) {
					for (CollectionChangeEvent.ElementChange<Anchor> el : evt.elements) {
						theScenarioAffectingChannels.add(el.index,
							theMetricService.getChannel(el.newValue, StandardMetrics.OF_INTEREST, null));
					}
				}
				break;
			case remove:
				try (Transaction sct = theScenarioAffectingChannels.lock(true, evt)) {
					for (CollectionChangeEvent.ElementChange<Anchor> el : evt.getElementsReversed()) {
						theScenarioAffectingChannels.remove(el.index);
					}
				}
				break;
			case set:
				break;
			}
		});
		class WatchedAnchor {
			final CollectionElement<Anchor> el;

			WatchedAnchor(CollectionElement<Anchor> el) {
				this.el = el;
			}
		}
		ObservableSortedSet<WatchedAnchor> watchedAnchors = theSelectedAnchorsCopy.flow()//
			.mapEquivalent(TypeToken.of(WatchedAnchor.class), // Convert to a structure that stores the element for performance
				a -> {
					try (Transaction t = theExistenceWatcher.lock()) {
						return new WatchedAnchor(theExistenceWatcher.watchAnchors2(a));
					}
				}, //
				wa -> wa.el.get(), //
				opts -> opts.cache(true).reEvalOnUpdate(false))//
			.filter(wa -> wa.el == null ? "Already Watched" : null)// If the element is null, it's a duplicate (possible?)
			.collect();
		// The mapping is what starts watching the anchors, but we have to un-watch the anchors manually
		watchedAnchors.changes().act(evt -> {
			switch (evt.type) {
			case remove:
				try (Transaction t = theExistenceWatcher.lock()) {
					for (CollectionChangeEvent.ElementChange<WatchedAnchor> wa : evt.getElementsReversed()) {
						theExistenceWatcher.unwatchAnchor2(wa.oldValue.el);
					}
				}
				break;
			default:
				break;
			}
		});
		theAnchors = watchedAnchors.flow()//
			.refresh(theExistenceUpdate)// Re-check the anchor's existence when this observable fires
			.filter(wa -> { // theAnchors only holds anchors that currently exist
				Boolean exists;
				try (Transaction t = theExistenceWatcher.lock()) {
					exists = theExistenceWatcher.getResult(wa.el, StandardMetrics.EXISTS).get();
				}
				if (Boolean.TRUE.equals(exists)) {
					return null;
				} else {
					return "Does not exist currently";
				}
			}).mapEquivalent(TypeToken.of(Anchor.class), wa -> wa.el.get(), Anchor::compareTo)// Map back to the anchor
			.collect();

		// Register pre-loaded components
		synchronized (theAssembly) {
			List<CurrentAnchorSetComponent> components = thePreLoadedComponents;
			thePreLoadedComponents = null;
			for (CurrentAnchorSetComponent component : components) {
				theAssembly.registerComponent(component, component.getAnchors(this));
			}
		}

		theMetricService.getSupport().changes().act(evt -> {
			supportChanged = true;
			envChange();
		});

		new Thread(() -> {
			while (true) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {}

				try {
					processChanges();
				} catch (RuntimeException | Error e) {
					System.err.println("Current state change processing error");
					e.printStackTrace();
				}
			}
		}, "Current Metric Updates").start();
	}

	void envChange() {
		if (!environmentChanged.getAndSet(true)) {
			theWork.workQueued(null, ENV_CHANGE_COST);
		}
	}

	@Override
	public MetricWork getWork() {
		return theWork;
	}

	final ConcurrentLinkedQueue<WatchedChannel<?>> channelsToRemove = new ConcurrentLinkedQueue<>();

	private class ChangeSet {
		final AtomicBoolean anyChanges;
		boolean changesThisRound;
		boolean envChanged;

		ChangeSet() {
			anyChanges = new AtomicBoolean();
		}

		void roundStart() {
			changesThisRound = false;
			envChanged = environmentChanged.compareAndSet(true, false);
		}

		void changed() {
			changesThisRound = true;
			if (anyChanges.compareAndSet(false, true)) {
				theWork.workQueued(null, NOTIFY_COST);
			}
		}

		void roundComplete() {
			if (envChanged) {
				theWork.workFinished(null, ENV_CHANGE_COST);
			}
			if (changesThisRound && DEBUG_CHANNELS) {
				System.out.println("Finished update iteration " + theCurrentUpdate);
			}
		}

		void finishCycle() {
			if (anyChanges.compareAndSet(true, false)) {
				theWork.workFinished(null, NOTIFY_COST);
			}
		}
	}

	private final ChangeSet theChangeSet = new ChangeSet();

	private void processChanges() {
		// Updates may cause all kinds of other changes, so do this in a loop until all update paths are finished
		do {
			theChangeSet.roundStart();
			boolean localSupportChanged = supportChanged;
			supportChanged = false;
			theCurrentUpdate++;
			Causable cause = Causable.simpleCause(null);

			try (Transaction ct = Causable.use(cause)) {
				if (localSupportChanged) {
					try (Transaction sact = theSelectedAnchorsCopy.lock(false, false, null);
						Transaction sct = theScenarioAffectingChannels.lock(true, false, cause)) {
						// Now we need to re-check the duration channels to see if the change in support means that the current "valid"
						// channel
						// for each anchor/metric has changed
						for (int a = 0; a < theSelectedAnchorsCopy.size(); a++) {
							CollectionElement<MetricChannel<Boolean>> el = theScenarioAffectingChannels.getElement(a);
							MetricChannel<Boolean> newChannel = theMetricService.getChannel(theSelectedAnchorsCopy.get(a),
								StandardMetrics.OF_INTEREST, null);
							if (!Objects.equals(newChannel, el.get())) {
								theScenarioAffectingChannels.mutableElement(el.getElementId()).set(newChannel);
							}
						}
					}
				}

				CollectionChangeEvent<Anchor> anchorChange = theSelectedAnchorChanges.poll();
				if (anchorChange != null) {
					theChangeSet.changed();
					long start = System.currentTimeMillis();
					try (Transaction at = theSelectedAnchorsCopy.lock(true, cause)) {
						while (anchorChange != null) {
							if (DEBUG_ANCHORS) {
								System.out.println("sac: " + anchorChange);
							}
							switch (anchorChange.type) {
							case add:
								for (CollectionChangeEvent.ElementChange<Anchor> el : anchorChange.elements) {
									theSelectedAnchorsCopy.add(el.index, el.newValue);
								}
								break;
							case remove:
								for (CollectionChangeEvent.ElementChange<Anchor> el : anchorChange.getElementsReversed()) {
									theSelectedAnchorsCopy.remove(el.index);
								}
								break;
							case set:
								break;
							}
							anchorChange = theSelectedAnchorChanges.poll();
						}
					}
					System.out.println("Anchor changes took " + QommonsUtils.printTimeLength(System.currentTimeMillis() - start));
				}

				// Since the WatchedChannel.check() method is what actually makes the queries and retrieves values,
				// this call tends to be very heavy.
				// We'll parallelize this work
				int queueSize = theChannelsToCheck.size();
				beginParallelCheck(queueSize);
				do {
					try {
						Thread.sleep(2);
					} catch (InterruptedException e) {}
				} while (isParallelChecking.get() > 0);
				for (WatchedChannel<?> toRemove : channelsToRemove) {
					// We have to keep checking this because someone could decide to listen for it any time
					if (toRemove.watchers.isEmpty()) {
						BiTuple<Anchor, Metric<?>> key = new BiTuple<>(toRemove.theAnchor, toRemove.theMetric);
						theWatchedChannels.remove(key);
						if (!toRemove.watchers.isEmpty()) {
							// Oops, someone decided to watch it. Put it back, quick!
							WatchedChannel<?> other = theWatchedChannels.put(key, toRemove);
							if (other != null) {
								// Someone decided to watch during the short time when the channel was not in the map
								// Refer them to the previously removed channel
								other.watchers.forEach(//
									w -> ((WatcherChannelAssoc<Object>) w).referTo((WatchedChannel<Object>) toRemove));
								other.unsubscribe();
							}
						} else {
							toRemove.unsubscribe();
						}
					}
				}
				channelsToRemove.clear();

				boolean[] updateFired = new boolean[1];
				theWatchers.forEach(//
					watcher -> {
						if (watcher.flushUpdates(cause)) {
							updateFired[0] = true;
						}
					});
				if (updateFired[0]) {
					theChangeSet.changed();
				}

				if (isExistenceChanged.getAndSet(false)) {
					theExistenceUpdate.onNext(cause);
					theWork.workFinished(null, EXISTENCE_CHANGE_COST);
				}
			}
			theChangeSet.roundComplete();
		} while (theChangeSet.changesThisRound);
		theChangeSet.finishCycle();
	}

	@Override
	public ObservableSortedSet<Anchor> getCurrentAnchors() {
		return theAnchors;
	}

	@Override
	public <A extends Anchor> ObservableSortedSet<A> getCurrentAnchors(AnchorType<A> type) {
		return (ObservableSortedSet<A>) theTypedAnchors.computeIfAbsent(type, this::_getAnchors);
	}

	private <A extends Anchor> ObservableSortedSet<A> _getAnchors(AnchorType<A> type) {
		DistinctSortedDataFlow<?, ?, A> flow = theAnchors.flow().filter(a -> a.getType().equals(type) ? null : StdMsg.ILLEGAL_ELEMENT)
			.mapEquivalent(type.getType(), a -> (A) a, a -> a, opts -> opts.cache(false));
		return flow.collect();
	}

	@Override
	public SimpleAnchorStateView createState() {
		return new ValueStateImpl();
	}

	@Override
	public AnchorMetricController createAnchorMetrics(List<AnchorSorter<?>> sorting, Observable<?> until) {
		return new AnchorMetricControllerImpl(sorting.isEmpty() ? Collections.emptyList() : new LinkedList<>(sorting), until);
	}

	private <T> WatcherChannelAssoc<T> addWatcher(Anchor anchor, Metric<T> metric, ValueStateImpl watcher) {
		WatchedChannel<T> channel = (WatchedChannel<T>) theWatchedChannels.computeIfAbsent(new BiTuple<>(anchor, metric),
			t -> new WatchedChannel<>(t.getValue1(), t.getValue2()));
		WatcherChannelAssoc<T> assoc = channel.addWatcher(watcher);
		if (channel.needsChannelCheck) {
			channel.queueForCheck(true, false);
		}
		return assoc;
	}

	private void beginParallelCheck(int queueSize) {
		// If at any point the environment changes, stop checking channels
		if (environmentChanged.get()) {
			return;
		}
		// I used to just serially create new channel checkers, adding them to the parallel queue before starting a new one,
		// but this can cause problems when a very few channels that are very heavy are being observed
		// With the serial method, it's likely that all those heavy channels would be put in the same ChannelChecker and so would be
		// executed serially on the same thread.
		// This method spreads the channels around better so that in the few very heavy channels case, we can still be very parallel
		WatchedChannel<?>[][] toCheck = new WatchedChannel[theChannelCheckers.length - (DEBUG_CHANNELS ? 1 : 0)][];
		int batchSize = Math.min((queueSize + toCheck.length - 1) / toCheck.length, 100);
		List<WatchedChannel<?>> debugCheck = DEBUG_CHANNELS ? new ArrayList<>() : null;
		WatchedChannel<?> ch = theChannelsToCheck.poll();
		while (ch != null) {
			// Begin a new set of batches
			for (int i = 0; i < toCheck.length; i++) {
				toCheck[i] = new WatchedChannel[batchSize];
			}
			// Populate the batches, spreading the channels out between batches
			int batch = 0;
			int index = 0;
			while (index < batchSize && ch != null) {
				if (ch.debug()) {
					debugCheck.add(ch);
				} else {
					toCheck[batch][index] = ch;
					batch++;
					if (batch == toCheck.length) {
						if (environmentChanged.get()) {
							break;
						}
						batch = 0;
						index++;
					}
				}
				ch = theChannelsToCheck.poll();
			}
			// Add the batches to the queue for execution (checking)
			for (int i = 0; i < toCheck.length; i++) {
				isParallelChecking.getAndIncrement();
				theWaitingCheckers.add(new ChannelChecker(toCheck[i]), false);
			}
		}
		// Last of all, check the debug channels
		if (debugCheck != null && !debugCheck.isEmpty()) {
			theDebugChecker.channels.add(new ChannelChecker(debugCheck.toArray(new WatchedChannel[debugCheck.size()])), false);
		}
	}

	private static final double ENV_CHANGE_COST = 1E-10;
	private static final double INIT_COST = 1E-9;
	private static final double NOTIFY_COST = 1E-7;
	private static final double EXISTENCE_CHANGE_COST = 1E-6;

	private class ChannelChecker {
		private final WatchedChannel<?>[] toCheck;

		ChannelChecker(WatchedChannel<?>[] toCheck) {
			this.toCheck = toCheck;
		}

		void check(ChannelCheckerThread thread) {
			try {
				for (WatchedChannel<?> ch : toCheck) {
					if (ch == null) {
						break;
					} else if (environmentChanged.get()) {
						theChannelsToCheck.add(ch);
					} else if (ch.watchers.isEmpty()) {
						channelsToRemove.add(ch);
					} else if (ch.dirty.getAndSet(false)) {
						theChangeSet.changed();
						ch.check(thread);
					}
				}
			} finally {
				isParallelChecking.decrementAndGet();
			}
		}
	}

	static final DBugAnchorType<WatchedChannel<?>> WATCHED_CHANNEL_DEBUG_TYPE = DBug.declare("metricity",
		(Class<WatchedChannel<?>>) (Class<?>) WatchedChannel.class, b -> b//
			.withStaticField("anchor", TypeTokens.get().of(Anchor.class), wc -> wc.theAnchor)//
			.withStaticField("metric", TypeTokens.get().of(Metric.class), wc -> wc.theMetric)//
			.withEvent("check", null)//
			.withEvent("noChannel", e -> e.withEventField("reason", TypeTokens.get().STRING))//
			.withEvent("query", null)//
			.withEvent("getValue", e -> e.withEventField("newQuery", TypeTokens.get().BOOLEAN))//
			.withEvent("gotValue",
				e -> e.withEventField("result", new TypeToken<MetricResult<?>>() {})
					.withEventField("oldResult", new TypeToken<MetricResult<?>>() {}).withEventField("hasResult", TypeTokens.get().BOOLEAN))//
			.withEvent("update", null)//
	);

	private class WatchedChannel<T> {
		final Anchor theAnchor;
		final Metric<T> theMetric;
		boolean needsChannelCheck;
		boolean queryBasisChange;
		MetricChannel<T> channel;
		final AtomicBoolean dirty;
		private final ListenerList<WatcherChannelAssoc<T>> watchers;

		private MetricQueryResult<T> currentResults;
		private MetricResult<T> theCurrentValue;
		private boolean isDebug;
		private final DBugAnchor<WatchedChannel<?>> debug;

		private volatile double theWorkCost;

		WatchedChannel(Anchor anchor, Metric<T> metric) {
			theAnchor = anchor;
			theMetric = metric;
			watchers = new ListenerList<>("Should not happen");
			theCurrentValue = MetricResult.COMPUTING();
			dirty = new AtomicBoolean();
			needsChannelCheck = true;
			theWorkCost = INIT_COST;

			if (DEBUG_CHANNELS) {}

			debug = WATCHED_CHANNEL_DEBUG_TYPE.debug(this).build();
		}

		void queueForCheck(boolean checkChannel, @SuppressWarnings("hiding") boolean queryBasisChange) {
			if (checkChannel) {
				needsChannelCheck = true;
			}
			if (queryBasisChange) {
				this.queryBasisChange = true;
			}
			if (!dirty.getAndSet(true)) {
				if (debug()) {
					System.out.println("CSS: " + this + " queue " + (checkChannel ? "channel check" : "update"));
				}
				theWork.workQueued(channel, theWorkCost);
				theChannelsToCheck.add(this);
			}
		}

		WatcherChannelAssoc<T> addWatcher(ValueStateImpl watcher) {
			return addWatcher(new WatcherChannelAssoc<>(this, watcher));
		}

		WatcherChannelAssoc<T> addWatcher(WatcherChannelAssoc<T> watcher) {
			watcher.unsubscribe = watchers.add(watcher, true);
			return watcher;
		}

		void check(ChannelCheckerThread thread) {
			if (debug()) {
				System.out.println("CSS: " + theAnchor + " check " + theCurrentUpdate + "(" + queryBasisChange + ")");
			}
			try (Transaction t = debug.event("check").begin()) {
				ValueHolder<String> reason = new ValueHolder<>();
				if (needsChannelCheck) {
					MetricChannel<T> newChannel = theMetricService.getChannel(theAnchor, theMetric, reason);
					if (debug()) {
						if (newChannel == null) {
							System.out.println("CSS: " + this + " channel nulled from " + channel);
						} else if (!needsChannelCheck) {
							System.out.println("CSS: " + this + " channel changed from " + channel + " to " + newChannel);
						}
					}
					if (newChannel == null && channel != null) {
						theWork.workQueued(channel, INIT_COST);
						theWork.workFinished(null, theWorkCost);
						theWorkCost = INIT_COST;
					}
					channel = newChannel;
				}
				if (channel == null) {
					if (debug() && !needsChannelCheck) {
						theMetricService.getChannel(theAnchor, theMetric, reason);
					}
					MetricResult<T> old = theCurrentValue;
					theCurrentValue = MetricResult.INVALID();
					if (!theCurrentValue.equals(old)) {
						notifyChanged();
					}
					debug.event("noChannel").with("reason", reason.get()).occurred();
					// Replace the old cost with the cost of a null channel
					return;
				}

				// This cost checking can be quite expensive for metrics with dynamic dependencies
				// So even though the cost is time-specific (because of dynamic dependencies),
				// it won't work to call it when only time changes
				if (needsChannelCheck || queryBasisChange) {
					double oldCost = theWorkCost;
					theWorkCost = theMetricService.getCost(channel);
					// Replace the initialization/old cost with the actual/new cost of this channel
					theWork.workQueued(channel, theWorkCost);
					theWork.workFinished(null, oldCost);
				}
				MetricResult<T> oldValue = theCurrentValue;
				thread.theCurrentJob = channel;
				try {
					boolean newQuery = false;
					if (!isSatisfactory(currentResults)) {
						currentResults = query();
						newQuery = true;
					} else if (needsChannelCheck && currentResults != null) {
						currentResults.checkDynamicDependencies();
					}
					updateCurrentValue(newQuery);
				} finally {
					thread.theCurrentJob = null;
				}
				if (!Objects.equals(oldValue, theCurrentValue)) {
					notifyChanged();
				}
			} catch (RuntimeException | Error e) {
				System.err.println("Error checking " + this);
				e.printStackTrace();
			} finally {
				theWork.workFinished(channel, theWorkCost);
				needsChannelCheck = false;
				queryBasisChange = false;
			}
		}

		boolean debug() {
			return isDebug;
		}

		private boolean isSatisfactory(MetricQueryResult<T> result) {
			if (result == null || !result.get().isValid())
				return false;
			else if (!result.getChannel().equals(channel))
				return false;
			return true;
		}

		private boolean isReusable(MetricQueryResult<T> timeline) {
			return timeline != null && timeline.getChannel().equals(channel);
		}

		private MetricQueryResult<T> query() {
			if (currentResults != null && !REUSE_TIMELINES) {
				currentResults.unsubscribe();
				currentResults = null;
			}
			if (debug()) {
				System.out.println("CSS: Querying " + this);
			}
			class WatchedChannelUpdater implements Consumer<Object> {
				MetricQueryResult<T> result;

				@Override
				public void accept(Object cause) {
					if (result == null || result != currentResults) {
						return;
					}
					handleUpdate(cause);
				}
			}
			WatchedChannelUpdater updater = new WatchedChannelUpdater();
			try (Transaction t = debug.event("query").begin()) {
				updater.result = theMetricService.query(channel, updater);
			}
			if (currentResults != null) {
				currentResults.unsubscribe();
				currentResults = null;
			}
			return updater.result;
		}

		void handleUpdate(Object cause) {
			if (debug()) {
				System.out.println("CSS: " + theAnchor + " handleUpdate (" + dirty + ")");
			}
			debug.event("update").occurred();
			queueForCheck(false, false);
		}

		private void updateCurrentValue(boolean newQuery) {
			if (debug()) {
				System.currentTimeMillis();
			}
			try (Transaction t = debug.event("getValue").with("newQuery", newQuery).begin()) {
				MetricQueryResult<T> timeline = currentResults;
				MetricResult<T> result = new MetricResult<>();
				MetricResult<T> old = theCurrentValue;
				if (timeline == null) {
					result.invalid();
					debug.event("gotValue").with("result", result).with("oldResult", old).with("hasTimeline", false).occurred();
					if (debug()) {
						System.out.println("CSS: " + theAnchor + " no timeline");
					}
				} else {
					timeline.getResult(result, MetricQueryOptions.get());
					// if (debug() && !newQuery && !result.isAvailable()) {
					// timeline.getResult(null, theTime, result);
					// }
					debug.event("gotValue").with("result", result).with("oldResult", old).with("hasResult", true).occurred();
					if (debug()) {
						System.out.println("CSS: " + theAnchor + " " + (newQuery ? "init" : "update") + " from " + old + " to " + result);
					}
				}
				theCurrentValue = result.immutable();
			} catch (RuntimeException | Error e) {
				System.err.println("Error getting value for " + channel);
				e.printStackTrace();
				theCurrentValue = MetricResult.INVALID();
			}
		}

		void unsubscribe() {
			if (debug()) {
				System.out.println("CSS: Unsubscribe " + this);
			}
			if (currentResults != null) {
				currentResults.unsubscribe();
				currentResults = null;
			}
			if (dirty.getAndSet(false)) {
				// Work has been queued, so we need to finish it
				theWork.workFinished(channel, theWorkCost);
			}
		}

		private void notifyChanged() {
			watchers.forEach(//
				watcher -> watcher.notifyChanged());
		}

		MetricResult<T> get() {
			if (debug() && !theCurrentValue.isAvailable()) {
				System.currentTimeMillis();
			}
			return theCurrentValue;
		}

		@Override
		public String toString() {
			return theAnchor + "." + theMetric;
		}
	}

	private class WatcherChannelAssoc<T> {
		WatchedChannel<T> channel;
		Runnable unsubscribe;
		final ValueStateImpl watcher;

		private boolean needsUpdate;

		WatcherChannelAssoc(WatchedChannel<T> channel, ValueStateImpl watcher) {
			this.channel = channel;
			this.watcher = watcher;
		}

		void referTo(WatchedChannel<T> newChannel) {
			unsubscribe.run();
			channel = newChannel;
			channel.addWatcher(this);
		}

		MetricResult<T> getResult() {
			return channel.get();
		}

		void remove() {
			unsubscribe.run();
			unsubscribe = null;
			if (channel.watchers.isEmpty()) {
				channel.queueForCheck(false, false);
			}
		}

		void notifyChanged() {
			if (!needsUpdate) {
				needsUpdate = true;
				watcher.theUpdatedChannels.add(this);
			}
		}

		boolean update() {
			needsUpdate = false;
			// If the channel is dirty again, we'll just have to be updated again
			return !channel.dirty.get();
		}

		@Override
		public String toString() {
			return channel.toString();
		}
	}

	private static class HashingIndexedSet<T> implements Iterable<T> {
		private final BetterHashMap<T, Integer> theDistinctValues;

		HashingIndexedSet() {
			theDistinctValues = BetterHashMap.build().unsafe().buildMap();
		}

		boolean isEmpty() {
			return theDistinctValues.isEmpty();
		}

		int size() {
			return theDistinctValues.size();
		}

		boolean ensureCapacity(int capacity) {
			return theDistinctValues.ensureCapacity(capacity);
		}

		CollectionElement<T> add(T value) {
			int size = theDistinctValues.size();
			MapEntryHandle<T, Integer> entry = theDistinctValues.getOrPutEntry(value, v -> size, false, null);
			if (entry.getValue().intValue() != size) {
				return null; // Already present
			}
			return theDistinctValues.keySet().getElement(entry.getElementId());
		}

		int remove(T value) {
			MapEntryHandle<T, Integer> entry = theDistinctValues.getEntry(value);
			if (entry == null) {
				return -1;// Not watched
			}
			CollectionElement<T> nextKeyEl = theDistinctValues.keySet().getAdjacentElement(entry.getElementId(), true);
			while (nextKeyEl != null) {
				MutableMapEntryHandle<T, Integer> next = theDistinctValues.mutableEntry(nextKeyEl.getElementId());
				next.setValue(next.get() - 1);
				nextKeyEl = theDistinctValues.keySet().getAdjacentElement(nextKeyEl.getElementId(), true);
			}
			int index = entry.get();
			theDistinctValues.mutableEntry(entry.getElementId()).remove();
			return index;
		}

		int remove(CollectionElement<T> element) {
			MapEntryHandle<T, Integer> entry = theDistinctValues.getEntryById(element.getElementId());
			CollectionElement<T> nextKeyEl = theDistinctValues.keySet().getAdjacentElement(entry.getElementId(), true);
			while (nextKeyEl != null) {
				MutableMapEntryHandle<T, Integer> next = theDistinctValues.mutableEntry(nextKeyEl.getElementId());
				next.setValue(next.get() - 1);
				nextKeyEl = theDistinctValues.keySet().getAdjacentElement(nextKeyEl.getElementId(), true);
			}
			int index = entry.get();
			theDistinctValues.mutableEntry(entry.getElementId()).remove();
			return index;
		}

		boolean contains(T value) {
			return theDistinctValues.containsKey(value);
		}

		int indexOf(T value) {
			Integer index = theDistinctValues.get(value);
			return index == null ? -1 : index;
		}

		int indexOf(CollectionElement<T> element) {
			return theDistinctValues.getEntryById(element.getElementId()).getValue();
		}

		void clear() {
			theDistinctValues.clear();
		}

		@Override
		public Iterator<T> iterator() {
			return theDistinctValues.keySet().iterator();
		}
	}

	private class ValueStateImpl implements SimpleAnchorStateView {
		private final ReentrantLock theWatchLock;
		// I was doing a sorted set on the anchors, but when a large number of anchors are watched, the sorting gets terrible
		// This way uses hashing, which is much faster
		// Plus, it's much harder to just do a sorted set on metrics because they're not naturally ordered.
		// So need to have the distinct map for containment tests and the ordered list for indexing into the table
		private final HashingIndexedSet<Anchor> theWatchedAnchors;
		private final HashingIndexedSet<Metric<?>> theWatchedMetrics;
		/** Indexed by anchor/metric */
		private final ArrayList<List<WatcherChannelAssoc<?>>> theChannels;
		private final ListenerList<Consumer<ValueStateChangeEvent>> theListeners;
		final ConcurrentLinkedQueue<WatcherChannelAssoc<?>> theUpdatedChannels;
		private NotificationLevel theNotificationLevel;
		private Runnable theRemove;

		ValueStateImpl() {
			theWatchLock = new ReentrantLock();
			theWatchedAnchors = new HashingIndexedSet<>();
			theWatchedMetrics = new HashingIndexedSet<>();
			theChannels = new ArrayList<>();
			Runnable[] remove = new Runnable[1];
			theListeners = new ListenerList<>("Reentrancy not allowed", true, inUse -> {
				// Only tell the service to notify us if someone is listening
				if (inUse) {
					remove[0] = theWatchers.add(this, true);
				} else {
					remove[0].run();
					remove[0] = null;
				}
			});
			theUpdatedChannels = new ConcurrentLinkedQueue<>();
			theNotificationLevel = NotificationLevel.VERBOSE;
		}

		private Transaction lock() {
			theWatchLock.lock();
			return theWatchLock::unlock;
		}

		@Override
		public SimpleAnchorStateView withNotifyLevel(NotificationLevel level) {
			theNotificationLevel = level;
			return this;
		}

		void prepAddAnchors(int count) {
			int newAnchorCount = theWatchedAnchors.size() + count;
			theChannels.ensureCapacity(newAnchorCount);
			theWatchedAnchors.ensureCapacity(newAnchorCount);
		}

		@Override
		public SimpleAnchorStateView watchAnchors(Collection<? extends Anchor> anchors) {
			if (anchors.isEmpty()) {
				return this;
			}
			try (Transaction t = lock()) {
				boolean wasEmpty = theWatchedAnchors.isEmpty();
				for (Anchor anchor : anchors) {
					if (theWatchedAnchors.add(anchor) == null) {
						continue; // Already watched
					}
					List<WatcherChannelAssoc<?>> newChannels = new ArrayList<>();
					theChannels.add(newChannels);
					for (Metric<?> metric : theWatchedMetrics) {
						newChannels.add(addWatcher(anchor, metric, this));
					}
				}
				if (wasEmpty && !theWatchedMetrics.isEmpty()) {
					theRemove = theWatchers.add(this, true);
				}
			}
			return this;
		}

		CollectionElement<Anchor> watchAnchors2(Anchor anchor) {
			boolean wasEmpty = theWatchedAnchors.isEmpty();
			CollectionElement<Anchor> el = theWatchedAnchors.add(anchor);
			if (el == null) {
				return null; // Already watched
			}
			List<WatcherChannelAssoc<?>> newChannels = new ArrayList<>();
			theChannels.add(newChannels);
			for (Metric<?> metric : theWatchedMetrics) {
				newChannels.add(addWatcher(anchor, metric, this));
			}
			if (wasEmpty && !theWatchedMetrics.isEmpty()) {
				theRemove = theWatchers.add(this, true);
			}
			return el;
		}

		@Override
		public SimpleAnchorStateView watchMetrics(Collection<? extends Metric<?>> metrics) {
			if (metrics.isEmpty()) {
				return this;
			}
			try (Transaction t = lock()) {
				boolean wasEmpty = theWatchedMetrics.isEmpty();
				for (Metric<?> metric : metrics) {
					if (theWatchedMetrics.add(metric) == null) {
						continue;
					}
					int a = 0;
					for (Anchor anchor : theWatchedAnchors) {
						// Metrics are always added at the end
						theChannels.get(a++).add(addWatcher(anchor, metric, this));
					}
				}
				if (wasEmpty && !theWatchedAnchors.isEmpty()) {
					theRemove = theWatchers.add(this, true);
				}
			}
			return this;
		}

		@Override
		public void unwatchAnchors(Collection<? extends Anchor> anchors) {
			if (anchors.isEmpty()) {
				return;
			}
			try (Transaction t = lock()) {
				boolean wasEmpty = theWatchedMetrics.isEmpty() || theWatchedAnchors.isEmpty();
				for (Anchor anchor : anchors) {
					int anchorIdx = theWatchedAnchors.remove(anchor);
					if (anchorIdx < 0) {
						continue; // Not watched
					}
					List<WatcherChannelAssoc<?>> channels = theChannels.remove(anchorIdx);
					for (WatcherChannelAssoc<?> channel : channels) {
						channel.remove();
					}
				}
				if (!wasEmpty && theWatchedAnchors.isEmpty()) {
					theRemove.run();
					theRemove = null;
				}
			}
		}

		void unwatchAnchor2(CollectionElement<Anchor> anchor) {
			boolean wasEmpty = theWatchedMetrics.isEmpty() || theWatchedAnchors.isEmpty();
			int anchorIdx = theWatchedAnchors.remove(anchor);
			List<WatcherChannelAssoc<?>> channels = theChannels.remove(anchorIdx);
			for (WatcherChannelAssoc<?> channel : channels) {
				channel.remove();
			}
			if (!wasEmpty && theWatchedAnchors.isEmpty()) {
				theRemove.run();
				theRemove = null;
			}
		}

		@Override
		public void unwatchMetrics(Collection<? extends Metric<?>> metrics) {
			if (metrics.isEmpty()) {
				return;
			}
			try (Transaction t = lock()) {
				boolean wasEmpty = theWatchedAnchors.isEmpty() || theWatchedMetrics.isEmpty();
				for (Metric<?> metric : metrics) {
					int metricIdx = theWatchedMetrics.remove(metric);
					for (List<WatcherChannelAssoc<?>> anchorChannel : theChannels) {
						anchorChannel.remove(metricIdx).remove();
					}
				}
				if (!wasEmpty && theWatchedMetrics.isEmpty()) {
					theRemove.run();
					theRemove = null;
				}
			}
		}

		@Override
		public void clear() {
			try (Transaction t = lock()) {
				boolean wasEmpty = theWatchedAnchors.isEmpty() || theWatchedMetrics.isEmpty();
				for (List<WatcherChannelAssoc<?>> anchorChannels : theChannels) {
					for (WatcherChannelAssoc<?> channel : anchorChannels) {
						channel.remove();
					}
				}
				theChannels.clear();
				theWatchedAnchors.clear();
				theWatchedMetrics.clear();
				if (!wasEmpty) {
					theRemove.run();
					theRemove = null;
				}
			}
		}

		protected <T> WatcherChannelAssoc<T> getChannel(Anchor anchor, Metric<T> metric) {
			WatcherChannelAssoc<T> channel;
			try (Transaction t = lock()) {
				int metricIdx = theWatchedMetrics.indexOf(metric);
				if (metricIdx < 0) {
					// Previously I was throwing these exceptions, but sometimes it would be tripped due to batching delays
					// throw new IllegalArgumentException("Metric " + metric + " is not being watched");
					return null;
				}
				int anchorIdx = theWatchedAnchors.indexOf(anchor);
				if (anchorIdx < 0) {
					// throw new IllegalArgumentException("Anchor " + anchor + " is not being watched");
					return null;
				}
				channel = (WatcherChannelAssoc<T>) theChannels.get(anchorIdx).get(metricIdx);
			}
			return channel;
		}

		@Override
		public <T> MetricResult<T> getResult(Anchor anchor, Metric<T> metric) {
			WatcherChannelAssoc<T> channel = getChannel(anchor, metric);
			if (channel == null) {
				return MetricResult.INVALID();
			} else {
				return channel.getResult();
			}
		}

		<T> MetricResult<T> getResult(CollectionElement<Anchor> anchorEl, Metric<T> metric) {
			// Assumed to already own the lock
			int metricIdx = theWatchedMetrics.indexOf(metric);
			if (metricIdx < 0) {
				// Previously I was throwing these exceptions, but sometimes it would be tripped due to batching delays
				// throw new IllegalArgumentException("Metric " + metric + " is not being watched");
				return MetricResult.INVALID();
			}
			int anchorIdx = theWatchedAnchors.indexOf(anchorEl);
			if (anchorIdx < 0) {
				// throw new IllegalArgumentException("Anchor " + anchor + " is not being watched");
				return MetricResult.INVALID();
			}
			WatcherChannelAssoc<T> channel = (WatcherChannelAssoc<T>) theChannels.get(anchorIdx).get(metricIdx);
			if (channel == null) {
				return MetricResult.INVALID();
			} else {
				return channel.getResult();
			}
		}

		@Override
		public Subscription onChange(Consumer<ValueStateChangeEvent> onChange) {
			return theListeners.add(onChange, true)::run;
		}

		/**
		 * @param cause The cause for the update
		 * @return Whether listeners were called with an update, or if it is known that more updates are needed
		 */
		boolean flushUpdates(Causable cause) {
			if (theUpdatedChannels.isEmpty()) {
				return false;
			}
			// Empty the updated channels into a separate list
			List<WatcherChannelAssoc<?>> updates = new ArrayList<>(theUpdatedChannels.size());
			Iterator<WatcherChannelAssoc<?>> iter = theUpdatedChannels.iterator();
			boolean ignoreIncomplete = theNotificationLevel != NotificationLevel.VERBOSE;
			boolean needsAllComplete = theNotificationLevel == NotificationLevel.COMPLETE_ALL;
			boolean allComplete = true;
			boolean hasListeners = !theListeners.isEmpty();
			boolean needAnotherRound = false;
			try (Transaction t = lock()) {
				while (iter.hasNext()) {
					WatcherChannelAssoc<?> channel = iter.next();
					if (!channel.update()) {
						needAnotherRound = true; // Need to flush these updates again next iteration
						continue;
					}
					if (!hasListeners) {
						// Nobody's listening, so don't compile updates to fire
					} else if (channel.unsubscribe == null) {
						// Not interested anymore
					} else if (ignoreIncomplete && channel.channel.get().isComputing()) {
						allComplete = false;
						if (needsAllComplete) {
							break;
						}
					} else {
						updates.add(channel);
					}
					iter.remove();
				}
			}
			if (!allComplete && needsAllComplete) {
				for (WatcherChannelAssoc<?> update : updates) {
					update.notifyChanged();
				}
				return true;
			}
			if (updates.isEmpty()) {
				return needAnotherRound;
			}

			ValueStateChangeEvent evt = new ValueStateChangeEvent(cause, //
				Collections.unmodifiableSet(updates.stream().map(update -> update.channel.theAnchor).collect(Collectors.toSet())),
				Collections.unmodifiableSet(updates.stream().map(update -> update.channel.theMetric).collect(Collectors.toSet())));

			try (Transaction t = Causable.use(evt)) {
				theListeners.forEach(//
					l -> l.accept(evt));
			} catch (RuntimeException | Error e) {
				System.out.println("Listener error");
				e.printStackTrace();
			}
			return true;
		}
	}

	private class AnchorMetricImpl implements AnchorMetric {
		private final CollectionElement<Anchor> theElement;
		private final AnchorMetricControllerImpl theController;
		private final SimpleObservable<Object> theUpdates;

		AnchorMetricImpl(CollectionElement<Anchor> anchor, AnchorMetricControllerImpl controller) {
			theElement = anchor;
			theController = controller;
			theUpdates = new SimpleObservable<>();
		}

		@Override
		public Anchor getAnchor() {
			return theElement.get();
		}

		@Override
		public <T> MetricResult<T> getResult(Metric<T> metric) {
			try (Transaction t = theController.theState.lock()) {
				if (!theElement.getElementId().isPresent()) {
					return MetricResult.INVALID();
				}
				return theController.theState.getResult(theElement, metric);
			}
		}

		@Override
		public Observable<?> updates() {
			return theUpdates.readOnly();
		}

		@Override
		public int compareTo(AnchorMetric o) {
			return theController.theCompare.compare(this, o);
		}

		void update(Object cause) {
			theUpdates.onNext(cause);
		}

		@Override
		public String toString() {
			return theElement.get().toString();
		}
	}

	private class AnchorMetricControllerImpl implements AnchorMetricController {
		private final ObservableSortedSet<AnchorMetricImpl> theAnchorMetrics;
		private final ObservableCollection<AnchorMetric> theExposedAnchorMetrics;
		private final Comparator<AnchorMetric> theCompare;
		final ValueStateImpl theState;
		private final SimpleObservable<Void> theKill;
		private final List<AnchorSorter<?>> theSorting;
		private boolean isFinished;

		AnchorMetricControllerImpl(List<AnchorSorter<?>> sorting, Observable<?> until) {
			theKill = new SimpleObservable<>();
			Observable<?> destruction = Observable.or(until, theKill);
			theAnchorMetrics = ObservableSortedSet.create(TypeTokens.get().of(AnchorMetricImpl.class),
				(a1, a2) -> a1.getAnchor().compareTo(a2.getAnchor()));
			theSorting = sorting;
			if (!theSorting.isEmpty()) {
				theCompare = (a1, a2) -> {
					int compare = 0;
					for (AnchorSorter<?> sorter : theSorting) {
						if (compare != 0) {
							break;
						}
						Object m1, m2;
						m1 = a1.get(sorter.sortMetric, null);
						m2 = a2.get(sorter.sortMetric, null);
						if (m1 == null && m2 == null) {
							compare = 0;
						} else if (m1 == null) {
							compare = 1; // Put the anchors with missing metrics at the end
						} else if (m2 == null) {
							compare = -1;
						} else {
							compare = ((Comparator<Object>) sorter.sorter).compare(m1, m2);
						}
					}
					if (compare == 0) {
						compare = a1.getAnchor().compareTo(a2.getAnchor());
					}
					return compare;
				};
			} else {
				theCompare = (a1, a2) -> a1.getAnchor().compareTo(a2.getAnchor());
			}
			DistinctSortedDataFlow<AnchorMetricImpl, ?, AnchorMetric> exposedFlow = theAnchorMetrics.flow()
				.mapEquivalent(TypeTokens.get().of(AnchorMetric.class), am -> am, am -> (AnchorMetricImpl) am, opts -> opts.cache(false));
			if (!theSorting.isEmpty()) {
				theExposedAnchorMetrics = exposedFlow.sorted(theCompare).unmodifiable().collectActive(destruction);
			} else {
				theExposedAnchorMetrics = exposedFlow.unmodifiable().collectPassive();
			}
			theState = (ValueStateImpl) createState();
			Subscription stateSub = theState.onChange(evt -> {
				try (Transaction t = theAnchorMetrics.lock(true, false, evt)) {
					theAnchorMetrics.spliterator().forEachElement(el -> {
						if (!evt.getAffectedAnchors().contains(el.get().getAnchor())) {
							return;
						}
						// Fire an update event
						theAnchorMetrics.mutableElement(el.getElementId()).set(el.get());
						el.get().update(evt);
					}, true);
				}
			});
			if (!theSorting.isEmpty()) {
				watchMetrics(theSorting.stream().map(s -> s.sortMetric).collect(Collectors.toList()));
			}
			destruction.act(v -> {
				stateSub.unsubscribe();
				clear();
			});
		}

		@Override
		public AnchorMetricController withNotifyLevel(NotificationLevel level) {
			theState.withNotifyLevel(level);
			return this;
		}

		@Override
		public AnchorMetricController watchAnchors(Collection<? extends Anchor> anchors) {
			if (anchors.isEmpty()) {
				return this;
			}
			if (isFinished) {
				throw new IllegalStateException("This controller is unsubscribed");
			}
			try (Transaction anchorT = theAnchorMetrics.lock(true, null); Transaction stateT = theState.lock()) {
				theState.prepAddAnchors(anchors.size());
				for (Anchor a : anchors) {
					CollectionElement<Anchor> el = theState.watchAnchors2(a);
					if (el == null) {
						continue;
					}
					AnchorMetricImpl am = new AnchorMetricImpl(el, this);
					theAnchorMetrics.add(am);
				}
			}
			return this;
		}

		@Override
		public AnchorMetricController watchMetrics(Collection<? extends Metric<?>> metrics) {
			if (isFinished) {
				throw new IllegalStateException("This controller is unsubscribed");
			}
			theState.watchMetrics(metrics);
			return this;
		}

		@Override
		public void unwatchAnchors(Collection<? extends Anchor> anchors) {
			try (Transaction t = theAnchorMetrics.lock(true, null); Transaction stateT = theState.lock()) {
				for (Anchor a : anchors) {
					CollectionElement<AnchorMetricImpl> el = theAnchorMetrics.search(am -> a.compareTo(am.getAnchor()),
						SortedSearchFilter.OnlyMatch);
					if (el != null) {
						theState.unwatchAnchor2(el.get().theElement);
						theAnchorMetrics.mutableElement(el.getElementId()).remove();
						continue;
					}
				}
			}
		}

		@Override
		public void unwatchMetrics(Collection<? extends Metric<?>> metrics) {
			// Don't let them remove metrics that we need for the sorting
			boolean removedAny = false;
			for (AnchorSorter<?> sorter : theSorting) {
				boolean needsRemove = metrics.contains(sorter.sortMetric);
				if (needsRemove) {
					// Can't remove this one, since we're using it internally
					if (!removedAny) {
						metrics = new HashSet<>(metrics);
						removedAny = true;
					}
					metrics.remove(sorter.sortMetric);
				}
			}
			theState.unwatchMetrics(metrics);
		}

		@Override
		public void clear() {
			theState.clear();
			try (Transaction t = theAnchorMetrics.lock(true, null)) {
				theAnchorMetrics.clear();
			}
		}

		@Override
		public void unsubscribe() {
			theKill.onNext(null);
			isFinished = true;
		}

		@Override
		public ObservableCollection<AnchorMetric> get() {
			return theExposedAnchorMetrics;
		}

		@Override
		public AnchorMetric get(Anchor anchor) {
			CollectionElement<AnchorMetricImpl> anchorMetric = theAnchorMetrics.search(am -> anchor.compareTo(am.getAnchor()),
				SortedSearchFilter.OnlyMatch);
			return anchorMetric == null ? null : anchorMetric.get();
		}
	}

	private class ChannelCheckerThread extends Thread {
		volatile MetricChannel<?> theCurrentJob;

		ChannelCheckerThread(ThreadGroup channelCheckerGroup, int jobIndex, boolean debug) {
			super(channelCheckerGroup, channelCheckerGroup.getName() + "[" + (debug ? "DEBUG" : "" + jobIndex) + "]");
			setDaemon(true);
			setPriority(Thread.MIN_PRIORITY);
		}

		@Override
		public void run() {
			while (true) {
				ChannelChecker toCheck = theWaitingCheckers.poll(Long.MAX_VALUE).get(); // Blocking retrieval
				toCheck.check(this);
			}
		}
	}

	private class DebugChannelCheckerThread extends ChannelCheckerThread {
		final ListenerList<ChannelChecker> channels;

		DebugChannelCheckerThread(ThreadGroup group) {
			super(group, theChannelCheckers.length, true);
			setDaemon(true);
			setPriority(Thread.MIN_PRIORITY);
			channels = ListenerList.build().withFastSize(false).build();
		}

		@Override
		public void run() {
			while (true) {
				ChannelChecker toCheck = channels.poll(Long.MAX_VALUE).get(); // Blocking retrieval
				toCheck.check(this);
			}
		}
	}
}
