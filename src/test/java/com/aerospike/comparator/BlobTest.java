package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

public class BlobTest extends AbstractBaseTest {

    @AfterEach
    void tearDown() {
        cleanupTestData();
    }

    public byte[] formBlob(int length, Random rand) {
        byte[] bytes = new byte[length];
        rand.nextBytes(bytes);
        return bytes;
    }

    @Test
    public void ignoreOptionTest() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key key = uniqueKey("blob");
        Random rand = ThreadLocalRandom.current();

        putRecord(TWO_CLUSTER_A, key,
                new Bin("name", "Tim"),
                new Bin("age", 312),
                new Bin("diff1", formBlob(10, rand)),
                new Bin("diff2", formBlob(30, rand)),
                new Bin("same", formBlob(10, new Random(1))));
        putRecord(TWO_CLUSTER_B, key,
                new Bin("name", "Tim"),
                new Bin("age", 312),
                new Bin("diff1", formBlob(10, rand)),
                new Bin("diff2", formBlob(30, rand)),
                new Bin("same", formBlob(10, new Random(1))));

        assertBinValuesDiffer(key, TWO_CLUSTER_A, TWO_CLUSTER_B, "diff1");
        assertBinValuesDiffer(key, TWO_CLUSTER_A, TWO_CLUSTER_B, "diff2");
        assertBinValuesEqual(key, TWO_CLUSTER_A, TWO_CLUSTER_B, "same");

        DifferenceSummary differences = runComparator(twoClusterScanArgs("RECORD_DIFFERENCES", null));
        assertTrue(differences.areDifferent());

        String fileName = writeYamlToFile("ignore.yaml",
                "---",
                "paths:",
                String.format("- path: /%s/%s/diff1", NAMESPACE, getTestSetName()),
                "  action: ignore",
                String.format("- path: /%s/%s/diff2", NAMESPACE, getTestSetName()),
                "  action: ignore",
                "");
        try {
            String[] args = twoClusterScanArgs("RECORD_DIFFERENCES", null);
            String[] argsWithPath = new String[args.length + 2];
            System.arraycopy(args, 0, argsWithPath, 0, args.length);
            argsWithPath[args.length] = "-pf";
            argsWithPath[args.length + 1] = fileName;

            differences = runComparator(argsWithPath);
            assertFalse(differences.areDifferent());
        }
        finally {
            removeFile(fileName);
        }
    }
}
