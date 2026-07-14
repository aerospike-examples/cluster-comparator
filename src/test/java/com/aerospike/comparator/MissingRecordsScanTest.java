package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

public class MissingRecordsScanTest extends AbstractBaseTest {

    @AfterEach
    void tearDown() {
        cleanupTestData();
    }

    @Test
    void twoClusters_detectsRecordMissingOnDestination() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key key = uniqueKey("missing");
        putRecord(TWO_CLUSTER_A, key, new Bin("value", "only-on-source"));

        assertRecordExists(TWO_CLUSTER_A, key);
        assertRecordMissing(TWO_CLUSTER_B, key);

        DifferenceSummary summary = runComparator(twoClusterScanArgs("MISSING_RECORDS", null));

        assertTrue(summary.areDifferent());
        assertEquals(0, summary.getMissingRecords(0));
        assertTrue(summary.getMissingRecords(1) > 0);
    }

    @Test
    void twoClusters_matchingRecordsReportNoMissing() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key key = uniqueKey("present");
        Bin[] bins = new Bin[] {new Bin("value", "same"), new Bin("count", 42)};
        putRecord(TWO_CLUSTER_A, key, bins);
        putRecord(TWO_CLUSTER_B, key, bins);

        assertRecordExists(TWO_CLUSTER_A, key);
        assertRecordExists(TWO_CLUSTER_B, key);
        assertBinValuesEqual(key, TWO_CLUSTER_A, TWO_CLUSTER_B, "value");

        DifferenceSummary summary = runComparator(twoClusterScanArgs("MISSING_RECORDS", null));

        assertFalse(summary.areDifferent());
        assertEquals(0, summary.getMissingRecords(0));
        assertEquals(0, summary.getMissingRecords(1));
    }

    @Test
    void twoClusters_detectsRecordMissingOnSource() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key key = uniqueKey("dest-only");
        putRecord(TWO_CLUSTER_B, key, new Bin("value", "only-on-destination"));

        assertRecordMissing(TWO_CLUSTER_A, key);
        assertRecordExists(TWO_CLUSTER_B, key);

        DifferenceSummary summary = runComparator(twoClusterScanArgs("MISSING_RECORDS", null));

        assertTrue(summary.areDifferent());
        assertTrue(summary.getMissingRecords(0) > 0);
        assertEquals(0, summary.getMissingRecords(1));
    }
}
