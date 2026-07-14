package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

public class ThreeClusterScanTest extends AbstractBaseTest {

    @AfterEach
    void tearDown() {
        cleanupTestData();
    }

    @Test
    void threeClusters_detectsRecordMissingOnOneCluster() throws Exception {
        assumeClustersAvailable(0, 1, 2);
        truncateTestSet(0, 1, 2);

        Key key = uniqueKey("three-missing");
        putRecord(0, key, new Bin("value", "on-0-and-1"));
        putRecord(1, key, new Bin("value", "on-0-and-1"));

        assertRecordExists(0, key);
        assertRecordExists(1, key);
        assertRecordMissing(2, key);

        String configFile = writeThreeClusterConfigFile();
        try {
            DifferenceSummary summary = runComparator(
                    threeClusterScanArgs(configFile, "MISSING_RECORDS", null));

            assertTrue(summary.areDifferent());
            assertEquals(0, summary.getMissingRecords(0));
            assertEquals(0, summary.getMissingRecords(1));
            assertTrue(summary.getMissingRecords(2) > 0);
        }
        finally {
            removeFile(configFile);
        }
    }

    @Test
    void threeClusters_identicalRecordsReportNoDifferences() throws Exception {
        assumeClustersAvailable(0, 1, 2);
        truncateTestSet(0, 1, 2);

        Key key = uniqueKey("three-same");
        Bin[] bins = new Bin[] {new Bin("value", "everywhere"), new Bin("n", 3)};
        putRecord(0, key, bins);
        putRecord(1, key, bins);
        putRecord(2, key, bins);

        assertBinValuesEqual(key, 0, 1, "value");
        assertBinValuesEqual(key, 0, 2, "value");

        String configFile = writeThreeClusterConfigFile();
        try {
            DifferenceSummary summary = runComparator(
                    threeClusterScanArgs(configFile, "RECORD_DIFFERENCES", null));
            assertFalse(summary.areDifferent());
        }
        finally {
            removeFile(configFile);
        }
    }

    @Test
    void threeClusters_detectsBinLevelDifferences() throws Exception {
        assumeClustersAvailable(0, 1, 2);
        truncateTestSet(0, 1, 2);

        Key key = uniqueKey("three-diff");
        putRecord(0, key, new Bin("shared", "yes"), new Bin("variant", "a"));
        putRecord(1, key, new Bin("shared", "yes"), new Bin("variant", "b"));
        putRecord(2, key, new Bin("shared", "yes"), new Bin("variant", "c"));

        assertBinValuesDiffer(key, 0, 1, "variant");
        assertBinValuesDiffer(key, 1, 2, "variant");

        String configFile = writeThreeClusterConfigFile();
        try {
            DifferenceSummary summary = runComparator(
                    threeClusterScanArgs(configFile, "RECORD_DIFFERENCES", null));
            assertTrue(summary.areDifferent());
        }
        finally {
            removeFile(configFile);
        }
    }
}
