package com.aerospike.comparator;

public interface ClusterNameResolver {
    String clusterIdToName(int id);
}