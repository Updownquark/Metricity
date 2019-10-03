package org.metricity.metric.service;

/** Options passed into a query */
public class MetricQueryOptions {
	private static final int NO_CACHE = 1;
	private static final MetricQueryOptions[] OPTIONS = new MetricQueryOptions[2];
	private static final MetricQueryOptions DEFAULT_OPTIONS = getOptions(0);

	/** @return Default query options (caching, synchronous, disconnected) */
	public static MetricQueryOptions get() {
		return DEFAULT_OPTIONS;
	}

	private static MetricQueryOptions getOptions(int status) {
		if (OPTIONS[status] == null)
			OPTIONS[status] = new MetricQueryOptions(status);
		return OPTIONS[status];
	}

	private final int theStatus;

	private MetricQueryOptions(int status) {
		theStatus = status;
	}

	/**
	 * @param useCache
	 *            Whether to use cached values in the query or ignore them
	 * @return The cache/no-cache query options
	 */
	public MetricQueryOptions useCache(boolean useCache) {
		if (useCache)
			return removeStatus(NO_CACHE);
		else
			return addStatus(NO_CACHE);
	}

	/** @return Whether this option set specifies cache usage */
	public boolean isUseCache() {
		return !getStatus(NO_CACHE);
	}

	private boolean getStatus(int mask) {
		return (theStatus & mask) != 0;
	}

	MetricQueryOptions addStatus(int mask) {
		return getOptions(theStatus | mask);
	}

	MetricQueryOptions removeStatus(int mask) {
		return getOptions(theStatus & ~mask);
	}
}
