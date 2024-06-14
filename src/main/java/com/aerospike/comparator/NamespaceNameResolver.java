
package com.aerospike.comparator;

public interface NamespaceNameResolver {
    /**
     * Namespaces can be translated. For example namespace A on cluster 1 might be known as namespace B on cluster 2. The translation YAML
     * file facilitates this mapping. When comparing namespaces, the "source" namesapce (A) will be given, but for each cluster the 
     * namespace must be looked up so the comparator can form valid keys using A on cluster 1 and B on cluster 2. This method provides that
     * mapping.  
     * @param name
     * @param clusterOrdinal
     * @return
     */
    String getNamespaceName(String name, int clusterOrdinal);
    
    /**
     * Translated namespace names can cause issues when being re-run. For example, say there are 3 clusters in the comparison and 
     * namespace A is known as A on cluster 1, B on cluster 2, and C on cluster 3. So the mapping file has
     * <pre>
     * A->B on cluster 2
     * A->C on cluster 3
     * </pre>
     * 
     * Suppose that a difference is found between clusters B and C.
     * Either B or C will be listed in the results, suppose B for this scenario. When a re-run is attempted, there is no way of knowing 
     * that B should be named C on the cluster 3 as the mapping does not contain this information. This method attempts to find the 
     * source namespace name for B (A) and then map it via the clusterOrdinal to the correct name (C)
     * @param name
     * @param clusterOrdinal
     * @return
     */
    String getNamespaceNameViaSource(String name, int clusterOrdinal);
}