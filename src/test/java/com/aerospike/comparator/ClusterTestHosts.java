package com.aerospike.comparator;

/**
 * Resolves integration-test cluster host addresses from environment variables or
 * system properties, falling back to the conventional local test ports.
 * <p>
 * Override per cluster with {@code AEROSPIKE_TEST_CLUSTER_1} (env) or
 * {@code aerospike.test.cluster.1} (system property). Clusters 2 and 3 use the
 * same pattern.
 */
public final class ClusterTestHosts {
    public static final int NUM_CLUSTERS = 3;
    private static final String[] DEFAULT_HOSTS = {
            "localhost:3100",
            "localhost:3101",
            "localhost:3102"
    };

    private ClusterTestHosts() {
    }

    public static String[] resolveAll() {
        String[] hosts = new String[NUM_CLUSTERS];
        for (int i = 0; i < NUM_CLUSTERS; i++) {
            hosts[i] = resolve(i);
        }
        return hosts;
    }

    public static String resolve(int clusterIndex) {
        if (clusterIndex < 0 || clusterIndex >= NUM_CLUSTERS) {
            throw new IllegalArgumentException(
                    String.format("clusterIndex must be between 0 and %d, not %d", NUM_CLUSTERS - 1, clusterIndex));
        }
        String propertyKey = "aerospike.test.cluster." + (clusterIndex + 1);
        String envKey = "AEROSPIKE_TEST_CLUSTER_" + (clusterIndex + 1);
        String value = System.getProperty(propertyKey);
        if (value == null || value.isBlank()) {
            value = System.getenv(envKey);
        }
        if (value == null || value.isBlank()) {
            return DEFAULT_HOSTS[clusterIndex];
        }
        return value.trim();
    }
}
