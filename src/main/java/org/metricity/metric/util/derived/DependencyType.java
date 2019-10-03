package org.metricity.metric.util.derived;

import java.util.function.Function;

/**
 * The degree to which a dependency is needed to perform the derived calculation
 * 
 * @author abutler
 */
public enum DependencyType implements Function<DependencyTypeSet, DependencyType> {
	/** Specifies that a dependency's result MUST be present to derive a valid result */
	Required,
	/** Specifies that a dependency's result may be used to augment the derived result, but that a valid result may be derived without it */
	Optional,
	/** Specifies that the dependency's result is actually not needed in the specific situation */
	None;

	@Override
	public DependencyType apply(DependencyTypeSet t) {
		return this;
	}
}
