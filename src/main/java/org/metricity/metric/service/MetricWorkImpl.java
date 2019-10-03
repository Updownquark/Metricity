package org.metricity.metric.service;

import java.util.function.Supplier;
import java.util.stream.Stream;

/** Simple implementation of {@link MetricWork} */
public class MetricWorkImpl extends AbstractWorkImpl {
	private final Supplier<Stream<MetricChannel<?>>> theCurrentWork;
	// final AtomicLong count = new AtomicLong(); // DEBUG

	/**
	 * @param threadSafe
	 *            Whether this structure should be thread-safe
	 * @param currentWork
	 *            The {@link MetricWork#getCurrentJobs()} implementation
	 */
	public MetricWorkImpl(boolean threadSafe, Supplier<Stream<MetricChannel<?>>> currentWork) {
		super(threadSafe);
		theCurrentWork = currentWork;
	}

	@Override
	public void workQueued(MetricChannel<?> channel, double weight) {
		// System.out.println("+" + weight + " for " + channel + ": " + count.incrementAndGet());
		super.workQueued(channel, weight);
	}

	@Override
	public void workFinished(MetricChannel<?> channel, double weight) {
		// System.out.println("-" + weight + " for " + channel + ": " + count.decrementAndGet());
		super.workFinished(channel, weight);
	}

	@Override
	public Stream<MetricChannel<?>> getCurrentJobs() {
		return theCurrentWork.get();
	}
}
