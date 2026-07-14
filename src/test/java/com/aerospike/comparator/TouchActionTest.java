package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

public class TouchActionTest extends AbstractBaseTest {

    @AfterEach
    void tearDown() {
        cleanupTestData();
    }

    @Test
    void twoClusters_touchActionIncrementsGenerationOnSourceCluster() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key key = uniqueKey("touch");
        putRecord(TWO_CLUSTER_A, key, new Bin("value", "touch-me"));
        assertRecordMissing(TWO_CLUSTER_B, key);

        String outputFile = writeToTempFile("touch-scan.csv");
        try {
            DifferenceSummary scanSummary = runComparator(twoClusterScanArgs("MISSING_RECORDS", outputFile));
            assertTrue(scanSummary.areDifferent());

            int generationBefore = getGeneration(TWO_CLUSTER_A, key);
            runComparator(twoClusterTouchArgs(outputFile));
            int generationAfter = getGeneration(TWO_CLUSTER_A, key);

            assertTrue(generationAfter > generationBefore,
                    "Touch action should increment generation on the cluster that holds the record");
            assertRecordMissing(TWO_CLUSTER_B, key);
        }
        finally {
            removeFile(outputFile);
        }
    }

    @Test
    void twoClusters_scanTouchIncrementsGenerationOnClusterWithRecord() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key key = uniqueKey("scan-touch");
        putRecord(TWO_CLUSTER_A, key, new Bin("value", "scan-touch-me"));
        assertRecordMissing(TWO_CLUSTER_B, key);

        int generationBefore = getGeneration(TWO_CLUSTER_A, key);

        String[] args = new String[] {
                "-h1", getHostString(TWO_CLUSTER_A),
                "-h2", getHostString(TWO_CLUSTER_B),
                "-n", NAMESPACE,
                "-s", getTestSetName(),
                "-a", "scan_touch",
                "-q",
                "-t", "1",
                "-C", "MISSING_RECORDS"
        };
        DifferenceSummary summary = runComparator(args);
        assertTrue(summary.areDifferent());

        int generationAfter = getGeneration(TWO_CLUSTER_A, key);
        assertTrue(generationAfter > generationBefore,
                "scan_touch should touch records on clusters that still hold the missing record");
        assertRecordMissing(TWO_CLUSTER_B, key);
    }

    private String writeToTempFile(String prefix) throws java.io.IOException {
        java.io.File tempFile = java.io.File.createTempFile(prefix, ".csv", new java.io.File(System.getProperty("java.io.tmpdir")));
        return tempFile.getAbsolutePath();
    }
}
