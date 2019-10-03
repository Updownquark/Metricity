package org.metricity.anchor;

import org.qommons.Named;
import org.qommons.StringUtils;

public interface AnchorSource extends Named, Comparable<AnchorSource> {
	@Override
	String getName();

	@Override
	default int compareTo(AnchorSource o) {
		if (this == o)
			return 0;
		return StringUtils.compareNumberTolerant(getName(), o.getName(), true, true);
	}
}
