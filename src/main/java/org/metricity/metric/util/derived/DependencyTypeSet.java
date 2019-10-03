package org.metricity.metric.util.derived;

/**
 * Information on dependency support for a derived metric satisfier
 * 
 * @author abutler
 */
public interface DependencyTypeSet {
	/**
	 * @param dependencyName
	 *            the name of the dependency
	 * @return Whether the given dependency is supported
	 */
	boolean isSupported(String dependencyName);
}
