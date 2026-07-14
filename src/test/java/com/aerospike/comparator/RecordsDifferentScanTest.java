package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

public class RecordsDifferentScanTest extends AbstractBaseTest {

    @AfterEach
    void tearDown() {
        cleanupTestData();
    }

    @Test
    void twoClusters_detectsContentDifferences() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key key = uniqueKey("content");
        putRecord(TWO_CLUSTER_A, key, new Bin("status", "active"), new Bin("version", 1));
        putRecord(TWO_CLUSTER_B, key, new Bin("status", "active"), new Bin("version", 2));

        assertBinValuesDiffer(key, TWO_CLUSTER_A, TWO_CLUSTER_B, "version");

        DifferenceSummary summary = runComparator(twoClusterScanArgs("RECORDS_DIFFERENT", null));
        assertTrue(summary.areDifferent());
        assertTrue(summary.getDifferences() > 0);
    }

    @Test
    void twoClusters_detectsMissingAndDifferentRecords() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key missingKey = uniqueKey("missing");
        Key differentKey = uniqueKey("different");

        putRecord(TWO_CLUSTER_A, missingKey, new Bin("marker", "missing"));
        putRecord(TWO_CLUSTER_A, differentKey, new Bin("marker", "left"));
        putRecord(TWO_CLUSTER_B, differentKey, new Bin("marker", "right"));

        assertRecordMissing(TWO_CLUSTER_B, missingKey);
        assertBinValuesDiffer(differentKey, TWO_CLUSTER_A, TWO_CLUSTER_B, "marker");

        DifferenceSummary summary = runComparator(twoClusterScanArgs("RECORDS_DIFFERENT", null));
        assertTrue(summary.areDifferent());
    }

    @Test
    void twoClusters_identicalRecordsReportNoDifferences() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key key = uniqueKey("identical");
        Bin[] bins = new Bin[] {new Bin("a", 1), new Bin("b", "two")};
        putRecord(TWO_CLUSTER_A, key, bins);
        putRecord(TWO_CLUSTER_B, key, bins);

        DifferenceSummary summary = runComparator(twoClusterScanArgs("RECORDS_DIFFERENT", null));
        assertFalse(summary.areDifferent());
    }
}
