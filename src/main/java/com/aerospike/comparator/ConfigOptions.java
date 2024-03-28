package com.aerospike.comparator;

import java.util.List;

public class ConfigOptions {
    private List<ClusterConfig> clusters;

    public List<ClusterConfig> getClusters() {
        return clusters;
    }

    public void setClusters(List<ClusterConfig> clusters) {
        this.clusters = clusters;
    }
}
