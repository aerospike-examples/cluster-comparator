package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

public class RecordDifferencesScanTest extends AbstractBaseTest {

    @AfterEach
    void tearDown() {
        cleanupTestData();
    }

    @Test
    void twoClusters_detectsBinLevelDifferences() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key key = uniqueKey("diff");
        putRecord(TWO_CLUSTER_A, key, new Bin("name", "Tim"), new Bin("age", 312), new Bin("city", "Sydney"));
        putRecord(TWO_CLUSTER_B, key, new Bin("name", "Tim"), new Bin("age", 312), new Bin("city", "Melbourne"));

        assertBinValuesEqual(key, TWO_CLUSTER_A, TWO_CLUSTER_B, "name");
        assertBinValuesDiffer(key, TWO_CLUSTER_A, TWO_CLUSTER_B, "city");

        DifferenceSummary summary = runComparator(twoClusterScanArgs("RECORD_DIFFERENCES", null));
        assertTrue(summary.areDifferent());
    }

    @Test
    void twoClusters_identicalRecordsReportNoDifferences() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key key = uniqueKey("same");
        Bin[] bins = new Bin[] {new Bin("name", "Alice"), new Bin("score", 99)};
        putRecord(TWO_CLUSTER_A, key, bins);
        putRecord(TWO_CLUSTER_B, key, bins);

        assertBinValuesEqual(key, TWO_CLUSTER_A, TWO_CLUSTER_B, "name");
        assertBinValuesEqual(key, TWO_CLUSTER_A, TWO_CLUSTER_B, "score");

        DifferenceSummary summary = runComparator(twoClusterScanArgs("RECORD_DIFFERENCES", null));
        assertFalse(summary.areDifferent());
    }

    @Test
    void twoClusters_ignorePathSuppressesConfiguredBinDifferences() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key key = uniqueKey("ignore");
        putRecord(TWO_CLUSTER_A, key, new Bin("keep", "same"), new Bin("ignoreMe", "alpha"));
        putRecord(TWO_CLUSTER_B, key, new Bin("keep", "same"), new Bin("ignoreMe", "beta"));

        assertBinValuesDiffer(key, TWO_CLUSTER_A, TWO_CLUSTER_B, "ignoreMe");

        DifferenceSummary withoutIgnore = runComparator(twoClusterScanArgs("RECORD_DIFFERENCES", null));
        assertTrue(withoutIgnore.areDifferent());

        String pathFile = writeYamlToFile("ignore.yaml",
                "---",
                "paths:",
                "- path: /" + NAMESPACE + "/" + getTestSetName() + "/ignoreMe",
                "  action: ignore",
                "");
        try {
            String[] args = twoClusterScanArgs("RECORD_DIFFERENCES", null);
            String[] argsWithPath = new String[args.length + 2];
            System.arraycopy(args, 0, argsWithPath, 0, args.length);
            argsWithPath[args.length] = "-pf";
            argsWithPath[args.length + 1] = pathFile;

            DifferenceSummary withIgnore = runComparator(argsWithPath);
            assertFalse(withIgnore.areDifferent());
        }
        finally {
            removeFile(pathFile);
        }
    }
}
