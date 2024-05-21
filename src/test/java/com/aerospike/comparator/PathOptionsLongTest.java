package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

public class PathOptionsLongTest extends AbstractBaseTest{
    @Test
    public void ignoreOptionTest() throws Exception {
        getClient(0).truncate(null, "test", SET_NAME, null);
        getClient(1).truncate(null, "test", SET_NAME, null);
        Key key = new Key("test", SET_NAME, 1);
        getClient(0).put(null, key, new Bin("name", "Tim"), new Bin("age", 312), new Bin("ignore", 27));
        getClient(1).put(null, key, new Bin("name", "Tim"), new Bin("age", 312), new Bin("ignore", 28));
        
        String[] args = new String[] {"-h1",getHostString(0), "-h2", getHostString(1), "-n", "test", 
                "-s", SET_NAME, "-a", "scan", "-c", "-t", "0", "-C", "RECORD_DIFFERENCES"};
        ClusterComparatorOptions options = new ClusterComparatorOptions(args);
        ClusterComparator comparator = new ClusterComparator(options);
        DifferenceSummary differences = comparator.begin();
        assertTrue(differences.areDifferent());

        // Specify a YAML file with ignore to test
        String fileName = writeYamlToFile("ignore.yaml", 
                "---",
                "paths:",
                String.format("- path: /test/%s/ignore", SET_NAME),
                "  action: ignore",
                "");
        try {
            args = new String[] {"-h1",getHostString(0), "-h2", getHostString(1), "-n", "test", 
                    "-s", SET_NAME, "-a", "scan", "-c", "-t", "0", "-C", "RECORD_DIFFERENCES", "-pf", fileName};
            options = new ClusterComparatorOptions(args);
            comparator = new ClusterComparator(options);
            differences = comparator.begin();
            assertFalse(differences.areDifferent());
        }
        catch (Exception e) {
            System.err.printf("Unexpected exception thrown: %s (%s)", e.getMessage(), e.getClass().getName());
            e.printStackTrace();
            throw e;
        }
        finally {
            getClient(0).delete(null, key);
            getClient(1).delete(null, key);
            removeFile(fileName);
        }
    }

    @Test
    public void ignoreOptionOnNestedPathTest() throws Exception {
        getClient(0).truncate(null, "test", SET_NAME, null);
        getClient(1).truncate(null, "test", SET_NAME, null);

        Key key = new Key("test", SET_NAME, 1);
        Map<String, String> map = new HashMap<>();
        map.put("abc", "123");
        map.put("def", "456");
        
        List<Object> list = new ArrayList<>();
        list.add("12345");
        list.add(map);
        getClient(0).put(null, key, new Bin("name", "Tim"), new Bin("age", 312), new Bin("list", list));
        map.put("def", "567");
        getClient(1).put(null, key, new Bin("name", "Tim"), new Bin("age", 312), new Bin("list", list));
        
        String[] args = new String[] {"-h1",getHostString(0), "-h2", getHostString(1), "-n", "test", 
                "-s", SET_NAME, "-a", "scan", "-c", "-t", "0", "-C", "RECORD_DIFFERENCES"};
        ClusterComparatorOptions options = new ClusterComparatorOptions(args);
        ClusterComparator comparator = new ClusterComparator(options);
        DifferenceSummary differences = comparator.begin();
        assertTrue(differences.areDifferent());

        // Specify a YAML file with ignore to test
        String fileName = writeYamlToFile("ignore.yaml", 
                "---",
                "paths:",
                String.format("- path: /test/%s/list/1/def", SET_NAME),
                "  action: ignore",
                "");
        try {
            args = new String[] {"-h1",getHostString(0), "-h2", getHostString(1), "-n", "test", 
                    "-s", SET_NAME, "-a", "scan", "-c", "-t", "0", "-C", "RECORD_DIFFERENCES", "-pf", fileName};
            options = new ClusterComparatorOptions(args);
            comparator = new ClusterComparator(options);
            differences = comparator.begin();
            assertFalse(differences.areDifferent());
        }
        catch (Exception e) {
            System.err.printf("Unexpected exception thrown: %s (%s)", e.getMessage(), e.getClass().getName());
            e.printStackTrace();
            throw e;
        }
        finally {
            getClient(0).delete(null, key);
            getClient(1).delete(null, key);
            removeFile(fileName);
        }
    }

    @Test
    public void unorderedOptionTest() throws Exception {
        getClient(0).truncate(null, "test", SET_NAME, null);
        getClient(1).truncate(null, "test", SET_NAME, null);

        Key key = new Key("test", SET_NAME, 1);
        getClient(0).put(null, key, new Bin("name", "Tim"), new Bin("age", 312), new Bin("unordered", List.of(1, 2, 3, 4, 5)));
        getClient(1).put(null, key, new Bin("name", "Tim"), new Bin("age", 312), new Bin("unordered", List.of(5, 1, 2, 3, 4)));
        
        String[] args = new String[] {"-h1",getHostString(0), "-h2", getHostString(1), "-n", "test", 
                "-s", SET_NAME, "-a", "scan", "-c", "-t", "0", "-C", "RECORD_DIFFERENCES"};
        ClusterComparatorOptions options = new ClusterComparatorOptions(args);
        ClusterComparator comparator = new ClusterComparator(options);
        DifferenceSummary differences = comparator.begin();
        assertTrue(differences.areDifferent());

        // Specify a YAML file with ignore to test
        String fileName = writeYamlToFile("unordered.yaml", 
                "---",
                "paths:",
                "- path: /test/*/unordered",
                "  action: compareUnordered",
                "");
        try {
            args = new String[] {"-h1",getHostString(0), "-h2", getHostString(1), "-n", "test", 
                    "-s", SET_NAME, "-a", "scan", "-c", "-t", "0", "-C", "RECORD_DIFFERENCES", "-pf", fileName};
            options = new ClusterComparatorOptions(args);
            comparator = new ClusterComparator(options);
            differences = comparator.begin();
            assertFalse(differences.areDifferent());
        }
        catch (Exception e) {
            System.err.printf("Unexpected exception thrown: %s (%s)", e.getMessage(), e.getClass().getName());
            e.printStackTrace();
            throw e;
        }
        finally {
            getClient(0).delete(null, key);
            getClient(1).delete(null, key);
            removeFile(fileName);
        }
    }
}
