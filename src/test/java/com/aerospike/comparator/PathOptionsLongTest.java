package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

public class PathOptionsLongTest extends AbstractBaseTest {

    @AfterEach
    void tearDown() {
        cleanupTestData();
    }

    @Test
    public void ignoreOptionTest() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key key = uniqueKey("long-ignore");
        putRecord(TWO_CLUSTER_A, key, new Bin("name", "Tim"), new Bin("age", 312), new Bin("ignore", 27L));
        putRecord(TWO_CLUSTER_B, key, new Bin("name", "Tim"), new Bin("age", 312), new Bin("ignore", 28L));

        assertBinValuesDiffer(key, TWO_CLUSTER_A, TWO_CLUSTER_B, "ignore");

        assertTrue(runComparator(twoClusterScanArgs("RECORD_DIFFERENCES", null)).areDifferent());

        String fileName = writeYamlToFile("ignore.yaml",
                "---",
                "paths:",
                String.format("- path: /%s/%s/ignore", NAMESPACE, getTestSetName()),
                "  action: ignore",
                "");
        try {
            String[] args = twoClusterScanArgs("RECORD_DIFFERENCES", null);
            String[] argsWithPath = new String[args.length + 2];
            System.arraycopy(args, 0, argsWithPath, 0, args.length);
            argsWithPath[args.length] = "-pf";
            argsWithPath[args.length + 1] = fileName;

            assertFalse(runComparator(argsWithPath).areDifferent());
        }
        finally {
            removeFile(fileName);
        }
    }

    @Test
    public void ignoreOptionOnNestedPathTest() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key key = uniqueKey("nested-ignore");
        Map<String, String> map = new HashMap<>();
        map.put("abc", "123");
        map.put("def", "456");

        List<Object> list = new ArrayList<>();
        list.add("12345");
        list.add(new HashMap<>(map));
        putRecord(TWO_CLUSTER_A, key, new Bin("name", "Tim"), new Bin("age", 312), new Bin("list", list));

        map.put("def", "567");
        list.set(1, new HashMap<>(map));
        putRecord(TWO_CLUSTER_B, key, new Bin("name", "Tim"), new Bin("age", 312), new Bin("list", list));

        assertTrue(runComparator(twoClusterScanArgs("RECORD_DIFFERENCES", null)).areDifferent());

        String fileName = writeYamlToFile("ignore.yaml",
                "---",
                "paths:",
                String.format("- path: /%s/%s/list/1/def", NAMESPACE, getTestSetName()),
                "  action: ignore",
                "");
        try {
            String[] args = twoClusterScanArgs("RECORD_DIFFERENCES", null);
            String[] argsWithPath = new String[args.length + 2];
            System.arraycopy(args, 0, argsWithPath, 0, args.length);
            argsWithPath[args.length] = "-pf";
            argsWithPath[args.length + 1] = fileName;

            assertFalse(runComparator(argsWithPath).areDifferent());
        }
        finally {
            removeFile(fileName);
        }
    }

    @Test
    public void unorderedOptionTest() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key key = uniqueKey("unordered");
        putRecord(TWO_CLUSTER_A, key, new Bin("name", "Tim"), new Bin("age", 312), new Bin("unordered", List.of(1, 2, 3, 4, 5)));
        putRecord(TWO_CLUSTER_B, key, new Bin("name", "Tim"), new Bin("age", 312), new Bin("unordered", List.of(5, 1, 2, 3, 4)));

        assertTrue(runComparator(twoClusterScanArgs("RECORD_DIFFERENCES", null)).areDifferent());

        String fileName = writeYamlToFile("unordered.yaml",
                "---",
                "paths:",
                "- path: /test/*/unordered",
                "  action: compareUnordered",
                "");
        try {
            String[] args = twoClusterScanArgs("RECORD_DIFFERENCES", null);
            String[] argsWithPath = new String[args.length + 2];
            System.arraycopy(args, 0, argsWithPath, 0, args.length);
            argsWithPath[args.length] = "-pf";
            argsWithPath[args.length + 1] = fileName;

            assertFalse(runComparator(argsWithPath).areDifferent());
        }
        finally {
            removeFile(fileName);
        }
    }
}
