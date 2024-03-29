package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListSortFlags;

public class ListTest {
    @Test
    public void TestList() throws Exception {
        String source = "172.17.0.2";
        String dest = "172.17.0.3";
        
        IAerospikeClient srcClient = new AerospikeClient(source, 3000);
        IAerospikeClient destClient = new AerospikeClient(dest, 3000);
        
        Key key = new Key("test", "testSet", 1);
        List<Integer> list = Arrays.asList(new Integer[] {9,1,2,8,3,7,4,6,5});
        List<Value> intList = new ArrayList<>();
        for (Integer i : list) {
            intList.add(Value.get(i));
        }
        srcClient.operate(null, key, ListOperation.create("ordered", ListOrder.ORDERED, false),
                ListOperation.create("unordered", ListOrder.UNORDERED, false),
                ListOperation.clear("ordered"),
                ListOperation.clear("unordered"),
                ListOperation.appendItems("ordered", intList),
                ListOperation.appendItems("unordered", intList));
        
        // Wait for XDR to ship the record.
        int count = 0;
        while (true) {
            Thread.sleep(500);
            Record rec = destClient.get(null, key);
            if (rec != null && rec.getList("unordered") != null && rec.getList("unordered").size() > 0 && ((long)rec.getList("unordered").get(0)) == 9) {
                break;
            }
            if (++count > 10) {
                throw new RuntimeException("No record arrival");
            }
        }
        // Sort the list on the destination so there's a difference
        destClient.operate(null, key, ListOperation.sort("unordered", ListSortFlags.DEFAULT));
        
        // Run the comparison. Default comparison should now fail.
        String[] args = new String[] {"-h1","172.17.0.2", "-h2", "172.17.0.3", "-n", "test", 
                "-s", "testSet", "-a", "scan", "-f", "/tmp/output.csv", "-c", "-t", "1", "-C", "RECORD_DIFFERENCES"};
        ClusterComparatorOptions options = new ClusterComparatorOptions(args);
        ClusterComparator comparator = new ClusterComparator(options);
        DifferenceSummary differences = comparator.begin();
        assertTrue(differences.areDifferent());
        //assertEquals(client, client);
        
        // Now re-create a comparator, ignoring the unordered list
        options.setPathOptions(new PathOptions(new PathOption("/test/testSet/unordered", PathAction.IGNORE)));
        comparator = new ClusterComparator(options);
        differences = comparator.begin();
        assertFalse(differences.areDifferent());
        
        // Now re-create a comparator, doing list compares of unordered in an unordered fashion
        options.setPathOptions(new PathOptions(new PathOption("/test/testSet/unordered", PathAction.COMPAREUNORDERED)));
        comparator = new ClusterComparator(options);
        differences = comparator.begin();
        assertFalse(differences.areDifferent());
        
        srcClient.close();
        destClient.close();
    }
}
