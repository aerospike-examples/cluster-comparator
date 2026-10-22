package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.comparator.dbaccess.LocalAerospikeClient;

/**
 * Live-cluster coverage for the paths that previously had only unit tests.
 * Each test skips when its clusters are not reachable.
 */
public class LiveGapScanTest extends AbstractBaseTest {

    @AfterEach
    void tearDown() {
        cleanupTestData();
    }

    @Test
    void quickCompare_reportsThePartitionOfARecordAddedOnOneSide() throws Exception {
        assumeTwoTestClustersAvailable();
        PartitionMap left = partitionMap(TWO_CLUSTER_A);
        PartitionMap right = partitionMap(TWO_CLUSTER_B);
        assumeTrue(left.isComplete(NAMESPACE) && right.isComplete(NAMESPACE),
                "partition-info is incomplete; quick compare cannot run");
        assumeTrue(!left.isMigrationsHappening(NAMESPACE) && !right.isMigrationsHappening(NAMESPACE),
                "migrations are in progress; quick compare cannot run");

        Key key = uniqueKey("quick");
        int partition = Partition.getPartitionId(key.digest);
        long netA = left.getNetObjectCount(NAMESPACE, partition);
        long netB = right.getNetObjectCount(NAMESPACE, partition);
        put(TWO_CLUSTER_A, key, false, new Bin("value", "quick-only"));
        if (netB == netA + 1) {
            put(TWO_CLUSTER_A, keyInPartition(partition), false, new Bin("value", "quick-extra"));
        }

        String output = tempFile("quick");
        try {
            runComparator(new String[] {
                    "-h1", getHostString(TWO_CLUSTER_A),
                    "-h2", getHostString(TWO_CLUSTER_B),
                    "-n", NAMESPACE,
                    "-a", "scan",
                    "-q",
                    "-t", "1",
                    "-C", "QUICK_NAMESPACE",
                    "-f", output
            });
            assertTrue(csvContainsPartition(output, partition),
                    "quick compare output should list partition " + partition);
        }
        finally {
            removeFile(output);
        }
    }

    @Test
    void setMapping_skipsKeylessRecordsAndReportsKeyedMisses() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();
        truncateSet(destinationSet(), TWO_CLUSTER_A, TWO_CLUSTER_B);

        String userKey = "mapped-" + System.nanoTime();
        Key sourceMatch = new Key(NAMESPACE, getTestSetName(), userKey);
        Key destMatch = new Key(NAMESPACE, destinationSet(), userKey);
        put(TWO_CLUSTER_A, sourceMatch, true, new Bin("value", "same"));
        put(TWO_CLUSTER_B, destMatch, true, new Bin("value", "same"));

        Key sourceOnly = new Key(NAMESPACE, getTestSetName(), "only-" + System.nanoTime());
        put(TWO_CLUSTER_A, sourceOnly, true, new Bin("value", "missing-on-dest"));

        Key destOnly = new Key(NAMESPACE, destinationSet(), "dest-only-" + System.nanoTime());
        put(TWO_CLUSTER_B, destOnly, true, new Bin("value", "invisible"));

        Key keyless = new Key(NAMESPACE, getTestSetName(), "keyless-" + System.nanoTime());
        put(TWO_CLUSTER_A, keyless, false, new Bin("value", "no-stored-key"));

        String config = writeYamlToFile("set-mapping.yaml",
                "---",
                "clusters:",
                "- hostName: " + getHostString(TWO_CLUSTER_A),
                "- hostName: " + getHostString(TWO_CLUSTER_B),
                "setMapping:",
                "- set: " + getTestSetName(),
                "  mappings:",
                "  - clusterIndex: 2",
                "    name: " + destinationSet());
        try {
            ClusterComparator comparator = new ClusterComparator(new ClusterComparatorOptions(new String[] {
                    "-cf", config,
                    "-n", NAMESPACE,
                    "-s", getTestSetName(),
                    "-a", "scan",
                    "-q",
                    "-t", "1",
                    "-C", "MISSING_RECORDS",
                    "-sc", "1"
            }));
            DifferenceSummary summary = comparator.begin();
            assertEquals(0, summary.getMissingRecords(0));
            assertEquals(1, summary.getMissingRecords(1));
            assertEquals(1, comparator.getRecordsSkippedMissingUserKey());
            assertRecordExists(TWO_CLUSTER_B, destOnly);
        }
        finally {
            removeFile(config);
            deleteQuietly(TWO_CLUSTER_A, sourceMatch, sourceOnly, keyless);
            deleteQuietly(TWO_CLUSTER_B, destMatch, destOnly);
        }
    }

    @Test
    void dateRange_verifiesRecordsThatExistOutsideTheWindow() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key key = uniqueKey("dated");
        put(TWO_CLUSTER_B, key, true, new Bin("value", "older"));
        long older = lastUpdateMs(TWO_CLUSTER_B, key);
        put(TWO_CLUSTER_A, key, true, new Bin("value", "newer"));
        long newer = lastUpdateMs(TWO_CLUSTER_A, key);
        if (newer <= older) {
            Thread.sleep(1100);
            put(TWO_CLUSTER_A, key, true, new Bin("value", "newer"));
            newer = lastUpdateMs(TWO_CLUSTER_A, key);
        }
        assumeTrue(newer > older, "could not separate the two last-update times");
        long begin = older + 1;

        DifferenceSummary verified = runComparator(dateArgs(begin, false));
        assertEquals(0, verified.getMissingRecords(0));
        assertEquals(0, verified.getMissingRecords(1));

        DifferenceSummary unverified = runComparator(dateArgs(begin, true));
        assertEquals(0, unverified.getMissingRecords(0));
        assertEquals(1, unverified.getMissingRecords(1));
    }

    @Test
    void dateRange_reportsARecordThatIsActuallyMissing() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key key = uniqueKey("dated-missing");
        put(TWO_CLUSTER_A, key, true, new Bin("value", "only-a"));
        long begin = lastUpdateMs(TWO_CLUSTER_A, key);

        DifferenceSummary summary = runComparator(dateArgs(begin, false));
        assertEquals(0, summary.getMissingRecords(0));
        assertEquals(1, summary.getMissingRecords(1));
    }

    @Test
    void rerun_readsShowMetadataFileWithACommaInTheKey() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key commaKey = new Key(NAMESPACE, getTestSetName(), "ke,y-" + System.nanoTime());
        Key plainKey = uniqueKey("plain");
        put(TWO_CLUSTER_A, commaKey, true, new Bin("value", "comma"));
        put(TWO_CLUSTER_A, plainKey, true, new Bin("value", "plain"));

        String scanFile = tempFile("rerun-in");
        String rerunFile = tempFile("rerun-out");
        try {
            DifferenceSummary scan = runComparator(rerunArgs("scan", scanFile, null));
            assertEquals(2, scan.getMissingRecords(1));
            assertTrue(Files.readString(new File(scanFile).toPath()).contains("\"ke,y-"));

            DifferenceSummary rerun = runComparator(rerunArgs("rerun", rerunFile, scanFile));
            assertEquals(0, rerun.getMissingRecords(0));
            assertEquals(2, rerun.getMissingRecords(1));
        }
        finally {
            removeFile(scanFile);
            removeFile(rerunFile);
            deleteQuietly(TWO_CLUSTER_A, commaKey);
        }
    }

    @Test
    void partitionList_scansOnlyTheListedPartition() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key included = uniqueKey("in-partition");
        int partition = Partition.getPartitionId(included.digest);
        Key excluded = keyInOtherPartition(partition);
        put(TWO_CLUSTER_A, included, true, new Bin("value", "listed"));
        put(TWO_CLUSTER_A, excluded, true, new Bin("value", "not-listed"));

        List<String> args = new ArrayList<>(Arrays.asList(twoClusterScanArgs("MISSING_RECORDS", null)));
        args.add("--partitionList");
        args.add(Integer.toString(partition));
        DifferenceSummary summary = runComparator(args.toArray(new String[0]));
        assertEquals(0, summary.getMissingRecords(0));
        assertEquals(1, summary.getMissingRecords(1));
    }

    @Test
    void remoteServer_reportsAMissingRecordOnTheFarCluster() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key key = uniqueKey("remote-missing");
        put(TWO_CLUSTER_A, key, true, new Bin("value", "local-only"));

        int port = freePort();
        AtomicReference<Throwable> serverError = new AtomicReference<>();
        Thread server = new Thread(() -> {
            try {
                runComparator(new String[] {
                        "-h1", getHostString(TWO_CLUSTER_B),
                        "-n", NAMESPACE,
                        "-s", getTestSetName(),
                        "-a", "scan",
                        "-q",
                        "--remoteServer", Integer.toString(port)
                });
            }
            catch (Throwable t) {
                serverError.set(t);
            }
        }, "live-remote-server");
        server.setDaemon(true);
        server.start();
        waitForPort(port, serverError);

        DifferenceSummary summary = runComparator(new String[] {
                "-h1", getHostString(TWO_CLUSTER_A),
                "-h2", "remote:127.0.0.1:" + port,
                "-n", NAMESPACE,
                "-s", getTestSetName(),
                "-a", "scan",
                "-q",
                "-t", "1",
                "-C", "MISSING_RECORDS"
        });
        assertEquals(0, summary.getMissingRecords(0));
        assertEquals(1, summary.getMissingRecords(1));
    }

    @Test
    void scanCustom_deleteRemovesTheRecordAndDurableDeleteLeavesATombstone() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key expunged = uniqueKey("expunge");
        put(TWO_CLUSTER_A, expunged, true, new Bin("value", "delete-me"));
        int expungePartition = Partition.getPartitionId(expunged.digest);
        long expungeTombstones = tombstones(TWO_CLUSTER_A, expungePartition);

        runComparator(customArgs("1:delete,2:none"));
        assertRecordMissing(TWO_CLUSTER_A, expunged);
        assertEquals(expungeTombstones, tombstones(TWO_CLUSTER_A, expungePartition));

        Key durable = uniqueKey("durable");
        put(TWO_CLUSTER_B, durable, true, new Bin("value", "durable-delete-me"));
        int durablePartition = Partition.getPartitionId(durable.digest);
        long durableTombstones = tombstones(TWO_CLUSTER_B, durablePartition);

        runComparator(customArgs("1:none,2:durable_delete"));
        assertRecordMissing(TWO_CLUSTER_B, durable);
        assertTrue(tombstones(TWO_CLUSTER_B, durablePartition) > durableTombstones,
                "durable delete should leave a tombstone");
    }

    private String[] dateArgs(long beginMillis, boolean skipVerify) {
        List<String> args = new ArrayList<>(Arrays.asList(twoClusterScanArgs("MISSING_RECORDS", null)));
        args.add("--beginDate");
        args.add(Long.toString(beginMillis));
        if (skipVerify) {
            args.add("--skipDateRangeVerify");
        }
        return args.toArray(new String[0]);
    }

    private String[] rerunArgs(String action, String outputFile, String inputFile) {
        List<String> args = new ArrayList<>(Arrays.asList(
                "-h1", getHostString(TWO_CLUSTER_A),
                "-h2", getHostString(TWO_CLUSTER_B),
                "-n", NAMESPACE,
                "-s", getTestSetName(),
                "-a", action,
                "-q",
                "-t", "1",
                "-C", "MISSING_RECORDS",
                "--showMetadata",
                "-f", outputFile));
        if (inputFile != null) {
            args.add("-i");
            args.add(inputFile);
        }
        return args.toArray(new String[0]);
    }

    private String[] customArgs(String actions) {
        return new String[] {
                "-h1", getHostString(TWO_CLUSTER_A),
                "-h2", getHostString(TWO_CLUSTER_B),
                "-n", NAMESPACE,
                "-s", getTestSetName(),
                "-a", "scan_custom",
                "-q",
                "-t", "1",
                "-C", "MISSING_RECORDS",
                "--skipChallenge",
                "--customActions", actions
        };
    }

    private void put(int clusterId, Key key, boolean sendKey, Bin... bins) {
        WritePolicy policy = new WritePolicy();
        policy.sendKey = sendKey;
        getClient(clusterId).put(policy, key, bins);
    }

    private long lastUpdateMs(int clusterId, Key key) {
        Record record = getClient(clusterId).operate(null, key,
                ExpOperation.read("lastUpdate", Exp.build(Exp.lastUpdate()), ExpReadFlags.EVAL_NO_FAIL));
        return record.getLong("lastUpdate") / 1_000_000L;
    }

    private PartitionMap partitionMap(int clusterId) {
        return new PartitionMap(new LocalAerospikeClient(getClient(clusterId)));
    }

    private long tombstones(int clusterId, int partition) {
        PartitionMap map = partitionMap(clusterId);
        assumeTrue(map.isComplete(NAMESPACE), "partition-info is incomplete");
        return map.getTombstones(NAMESPACE, partition);
    }

    private Key keyInPartition(int partition) {
        for (int i = 0; i < 10000; i++) {
            Key key = uniqueKey("part-" + i);
            if (Partition.getPartitionId(key.digest) == partition) {
                return key;
            }
        }
        throw new IllegalStateException("could not find a second key in partition " + partition);
    }

    private Key keyInOtherPartition(int partition) {
        for (int i = 0; i < 10000; i++) {
            Key key = uniqueKey("other-" + i);
            if (Partition.getPartitionId(key.digest) != partition) {
                return key;
            }
        }
        throw new IllegalStateException("could not find a key outside partition " + partition);
    }

    private static boolean csvContainsPartition(String file, int partition) throws IOException {
        List<String> lines = Files.readAllLines(new File(file).toPath());
        for (int i = 1; i < lines.size(); i++) {
            List<String> fields = FileLine.splitCsv(lines.get(i));
            if (fields.size() > 2 && Integer.toString(partition).equals(fields.get(2))) {
                return true;
            }
        }
        return false;
    }

    private void truncateSet(String setName, int... clusterIds) {
        for (int clusterId : clusterIds) {
            getClient(clusterId).truncate(null, NAMESPACE, setName, null);
        }
    }

    private void deleteQuietly(int clusterId, Key... keys) {
        for (Key key : keys) {
            try {
                getClient(clusterId).delete(null, key);
            }
            catch (RuntimeException ignored) {
                // The record may already be gone.
            }
        }
    }

    private String destinationSet() {
        return getTestSetName() + "_dest";
    }

    private static String tempFile(String prefix) throws IOException {
        return File.createTempFile(prefix, ".csv", new File(System.getProperty("java.io.tmpdir"))).getAbsolutePath();
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void waitForPort(int port, AtomicReference<Throwable> serverError) throws Exception {
        long deadline = System.currentTimeMillis() + 10000;
        while (System.currentTimeMillis() < deadline) {
            if (serverError.get() != null) {
                throw new AssertionError("remote server failed to start", serverError.get());
            }
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", port), 200);
                return;
            }
            catch (IOException ignored) {
                Thread.sleep(100);
            }
        }
        throw new AssertionError("remote server did not listen on port " + port, serverError.get());
    }
}
