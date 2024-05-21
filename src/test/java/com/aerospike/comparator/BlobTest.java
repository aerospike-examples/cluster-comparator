package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

public class BlobTest extends AbstractBaseTest {
    public byte[] formBlob(int length, Random rand) {
        byte[] bytes = new byte[length];
        rand.nextBytes(bytes);
        return bytes;
    }
    
    @Test
    public void ignoreOptionTest() throws Exception {
        getClient(0).truncate(null, "test", SET_NAME, null);
        getClient(1).truncate(null, "test", SET_NAME, null);
        Key key = new Key("test", SET_NAME, 1);
        Random rand = ThreadLocalRandom.current();
        
        getClient(0).put(null, key, new Bin("name", "Tim"), new Bin("age", 312), new Bin("diff1", formBlob(10, rand)), new Bin("diff2", formBlob(30, rand)), new Bin("same", formBlob(10, new Random(1))));
        getClient(1).put(null, key, new Bin("name", "Tim"), new Bin("age", 312), new Bin("diff1", formBlob(10, rand)), new Bin("diff2", formBlob(30, rand)), new Bin("same", formBlob(10, new Random(1))));
        
        
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
                String.format("- path: /test/%s/diff1", SET_NAME),
                "  action: ignore",
                String.format("- path: /test/%s/diff2", SET_NAME),
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

}
