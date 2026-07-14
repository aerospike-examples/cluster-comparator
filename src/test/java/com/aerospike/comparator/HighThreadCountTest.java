package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

/**
 * Verifies high thread counts work end-to-end, including Aerospike client connection sizing.
 */
public class HighThreadCountTest extends AbstractBaseTest {

    @AfterEach
    void tearDown() {
        cleanupTestData();
    }

    @Test
    void twoClusters_with150Threads_detectsMissingRecord() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key key = uniqueKey("high-threads");
        putRecord(TWO_CLUSTER_A, key, new Bin("value", "only-on-source"));

        assertRecordExists(TWO_CLUSTER_A, key);
        assertRecordMissing(TWO_CLUSTER_B, key);

        DifferenceSummary summary = runComparator(twoClusterScanArgs("MISSING_RECORDS", null, 150));

        assertTrue(summary.areDifferent());
        assertEquals(0, summary.getMissingRecords(0));
        assertEquals(1, summary.getMissingRecords(1));
    }
}
