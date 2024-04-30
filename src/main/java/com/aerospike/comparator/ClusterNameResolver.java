package com.aerospike.comparator;

public interface ClusterNameResolver {
    /**
     * return true if 0 <= id < number_of_clusters
     * @param id
     * @return
     */
    boolean isClusterIdValid(int id);
    /**
     * Returns the cluster name (in double quotes) of the cluster at the 0-based index. If the specified cluster does not have a name, a string
     * representing the ordinal number plus 1 is returned, not in quotes. If the index is out of range of the passed clusters, an exception is thrown.
     * @param id
     * @return
     */
    String clusterIdToName(int id);
    /**
     * Return the 0-based index of the cluster with the passed name. If the cluster name does not exist, -1 is returned.
     */
    int clusterNameToId(String name);
    /**
     * Returns the number of clusters in this comparison
     * @return
     */
    int getNumberOfClusters();
}