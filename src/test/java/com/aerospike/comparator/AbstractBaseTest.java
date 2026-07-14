package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Node;

/**
 * Base class for integration tests that require live Aerospike clusters.
 * Cluster addresses are configurable via {@link ClusterTestHosts}.
 */
@Tag("integration")
public abstract class AbstractBaseTest {
    public static final int NUM_CLUSTERS = ClusterTestHosts.NUM_CLUSTERS;
    public static final String NAMESPACE = "test";
    public static final String SET_NAME_PREFIX = "compTest_";

    /** Zero-based index for ordinal cluster 2 (default {@code localhost:3101}). */
    public static final int TWO_CLUSTER_A = 1;
    /** Zero-based index for ordinal cluster 3 (default {@code localhost:3102}). */
    public static final int TWO_CLUSTER_B = 2;

    private static final String[] HOSTS = ClusterTestHosts.resolveAll();
    private static final IAerospikeClient[] SHARED_CLIENTS = new IAerospikeClient[NUM_CLUSTERS];
    private static final boolean[] CLUSTER_AVAILABLE = new boolean[NUM_CLUSTERS];
    private static boolean clustersValidated;

    private final List<Key> keysToCleanup = new ArrayList<>();

    @BeforeAll
    static void probeAllClusters() {
        if (clustersValidated) {
            return;
        }
        for (int i = 0; i < NUM_CLUSTERS; i++) {
            CLUSTER_AVAILABLE[i] = probeCluster(i);
        }
        clustersValidated = true;
    }

    @AfterAll
    static void closeSharedClients() {
        for (int i = 0; i < NUM_CLUSTERS; i++) {
            if (SHARED_CLIENTS[i] != null) {
                SHARED_CLIENTS[i].close();
                SHARED_CLIENTS[i] = null;
            }
        }
        clustersValidated = false;
    }

    protected static void assumeTwoTestClustersAvailable() {
        assumeClustersAvailable(TWO_CLUSTER_A, TWO_CLUSTER_B);
    }

    protected void truncateTwoTestClusters() {
        truncateTestSet(TWO_CLUSTER_A, TWO_CLUSTER_B);
    }

    protected static void assumeClustersAvailable(int... clusterIds) {
        probeAllClusters();
        for (int clusterId : clusterIds) {
            assumeTrue(CLUSTER_AVAILABLE[clusterId],
                    String.format("Cluster %d (%s) is not available", clusterId + 1, HOSTS[clusterId]));
        }
    }

    protected String getTestSetName() {
        return SET_NAME_PREFIX + getClass().getSimpleName();
    }

    protected String getHostString(int clusterId) {
        validateClusterId(clusterId);
        return HOSTS[clusterId];
    }

    protected IAerospikeClient getClient(int clusterId) {
        validateClusterId(clusterId);
        if (SHARED_CLIENTS[clusterId] == null) {
            SHARED_CLIENTS[clusterId] = new AerospikeClient(null, Host.parseHosts(HOSTS[clusterId], 3000));
        }
        return SHARED_CLIENTS[clusterId];
    }

    protected Key uniqueKey(String suffix) {
        long id = ThreadLocalRandom.current().nextLong(1_000_000_000L, Long.MAX_VALUE);
        Key key = new Key(NAMESPACE, getTestSetName(), suffix + "-" + id);
        keysToCleanup.add(key);
        return key;
    }

    protected void truncateTestSet(int... clusterIds) {
        for (int clusterId : clusterIds) {
            getClient(clusterId).truncate(null, NAMESPACE, getTestSetName(), null);
        }
    }

    protected void putRecord(int clusterId, Key key, Bin... bins) {
        getClient(clusterId).put(null, key, bins);
    }

    protected Record getRecord(int clusterId, Key key) {
        return getClient(clusterId).get(null, key);
    }

    protected void assertRecordExists(int clusterId, Key key) {
        assertNotNull(getRecord(clusterId, key),
                String.format("Expected record %s on cluster %d (%s)", key, clusterId + 1, getHostString(clusterId)));
    }

    protected void assertRecordMissing(int clusterId, Key key) {
        assertNull(getRecord(clusterId, key),
                String.format("Expected record %s to be absent on cluster %d (%s)", key, clusterId + 1, getHostString(clusterId)));
    }

    protected void assertBinValuesDiffer(Key key, int clusterA, int clusterB, String binName) {
        Record recordA = getRecord(clusterA, key);
        Record recordB = getRecord(clusterB, key);
        assertNotNull(recordA, String.format("Record %s missing on cluster %d", key, clusterA + 1));
        assertNotNull(recordB, String.format("Record %s missing on cluster %d", key, clusterB + 1));
        Object valueA = recordA.getValue(binName);
        Object valueB = recordB.getValue(binName);
        assertNotNull(valueA, String.format("Bin %s missing on cluster %d", binName, clusterA + 1));
        assertNotNull(valueB, String.format("Bin %s missing on cluster %d", binName, clusterB + 1));
        assertNotEquals(valueA, valueB,
                String.format("Bin %s should differ between cluster %d and %d (possible XDR replication)", binName, clusterA + 1, clusterB + 1));
    }

    protected void assertBinValuesEqual(Key key, int clusterA, int clusterB, String binName) {
        Record recordA = getRecord(clusterA, key);
        Record recordB = getRecord(clusterB, key);
        assertNotNull(recordA, String.format("Record %s missing on cluster %d", key, clusterA + 1));
        assertNotNull(recordB, String.format("Record %s missing on cluster %d", key, clusterB + 1));
        Object valueA = recordA.getValue(binName);
        Object valueB = recordB.getValue(binName);
        if (valueA instanceof byte[] && valueB instanceof byte[]) {
            assertTrue(Arrays.equals((byte[]) valueA, (byte[]) valueB),
                    String.format("Bin %s should match between cluster %d and %d", binName, clusterA + 1, clusterB + 1));
        }
        else {
            assertEquals(valueA, valueB,
                    String.format("Bin %s should match between cluster %d and %d", binName, clusterA + 1, clusterB + 1));
        }
    }

    protected int getGeneration(int clusterId, Key key) {
        Record record = getRecord(clusterId, key);
        return record == null ? -1 : record.generation;
    }

    protected DifferenceSummary runComparator(String[] args) throws Exception {
        ClusterComparatorOptions options = new ClusterComparatorOptions(args);
        ClusterComparator comparator = new ClusterComparator(options);
        return comparator.begin();
    }

    protected String[] twoClusterScanArgs(String compareMode, String outputFile) {
        return twoClusterScanArgs(TWO_CLUSTER_A, TWO_CLUSTER_B, compareMode, outputFile, 1);
    }

    protected String[] twoClusterScanArgs(String compareMode, String outputFile, int threads) {
        return twoClusterScanArgs(TWO_CLUSTER_A, TWO_CLUSTER_B, compareMode, outputFile, threads);
    }

    protected String[] twoClusterScanArgs(int clusterA, int clusterB, String compareMode, String outputFile) {
        return twoClusterScanArgs(clusterA, clusterB, compareMode, outputFile, 1);
    }

    protected String[] twoClusterScanArgs(int clusterA, int clusterB, String compareMode, String outputFile, int threads) {
        List<String> args = new ArrayList<>(Arrays.asList(
                "-h1", getHostString(clusterA),
                "-h2", getHostString(clusterB),
                "-n", NAMESPACE,
                "-s", getTestSetName(),
                "-a", "scan",
                "-q",
                "-t", String.valueOf(threads),
                "-C", compareMode));
        if (outputFile != null) {
            args.add("-f");
            args.add(outputFile);
        }
        return args.toArray(new String[0]);
    }

    protected String writeThreeClusterConfigFile() throws IOException {
        return writeYamlToFile("clusters.yaml",
                "---",
                "clusters:",
                "- hostName: " + getHostString(0),
                "- hostName: " + getHostString(1),
                "- hostName: " + getHostString(2),
                "");
    }

    protected String[] threeClusterScanArgs(String configFile, String compareMode, String outputFile) {
        List<String> args = new ArrayList<>(Arrays.asList(
                "-cf", configFile,
                "-n", NAMESPACE,
                "-s", getTestSetName(),
                "-a", "scan",
                "-q",
                "-t", "1",
                "-C", compareMode));
        if (outputFile != null) {
            args.add("-f");
            args.add(outputFile);
        }
        return args.toArray(new String[0]);
    }

    protected String[] twoClusterTouchArgs(String inputFile) {
        return twoClusterTouchArgs(TWO_CLUSTER_A, TWO_CLUSTER_B, inputFile);
    }

    protected String[] twoClusterTouchArgs(int clusterA, int clusterB, String inputFile) {
        return new String[] {
                "-h1", getHostString(clusterA),
                "-h2", getHostString(clusterB),
                "-n", NAMESPACE,
                "-s", getTestSetName(),
                "-a", "touch",
                "-q",
                "-i", inputFile
        };
    }

    protected void cleanupTestData() {
        for (Key key : keysToCleanup) {
            for (int i = 0; i < NUM_CLUSTERS; i++) {
                if (CLUSTER_AVAILABLE[i]) {
                    try {
                        getClient(i).delete(null, key);
                    }
                    catch (AerospikeException ignored) {
                        // Record may not exist on this cluster.
                    }
                }
            }
        }
        keysToCleanup.clear();
    }

    protected boolean removeFile(String filename) {
        return new File(filename).delete();
    }

    protected String writeYamlToFile(String fileName, String... lines) throws IOException {
        String newLine = System.getProperty("line.separator");
        String linesToWrite = Arrays.stream(lines).collect(Collectors.joining(newLine));
        return writeToFile(fileName, linesToWrite);
    }

    private String writeToFile(String filenamePrefix, String data) throws IOException {
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        File tempFile = File.createTempFile(filenamePrefix, ".tmp", tempDir);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
            writer.write(data);
        }
        return tempFile.getAbsolutePath();
    }

    private static boolean probeCluster(int clusterIndex) {
        IAerospikeClient client = null;
        try {
            client = new AerospikeClient(null, Host.parseHosts(HOSTS[clusterIndex], 3000));
            for (int attempt = 0; attempt < 20; attempt++) {
                if (client.isConnected()) {
                    Node[] nodes = client.getNodes();
                    if (nodes != null && nodes.length > 0) {
                        return true;
                    }
                }
                Thread.sleep(250);
            }
            return false;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        catch (RuntimeException e) {
            return false;
        }
        finally {
            if (client != null) {
                client.close();
            }
        }
    }

    private void validateClusterId(int clusterId) {
        if (clusterId < 0 || clusterId >= NUM_CLUSTERS) {
            throw new IllegalArgumentException(String.format(
                    "clusterId must be between 0 and %d, not %d", NUM_CLUSTERS - 1, clusterId));
        }
    }
}
