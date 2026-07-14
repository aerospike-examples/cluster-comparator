package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListSortFlags;

public class ListTest extends AbstractBaseTest {

    @AfterEach
    void tearDown() {
        cleanupTestData();
    }

    @Test
    public void testListComparisonModes() throws Exception {
        assumeTwoTestClustersAvailable();
        truncateTwoTestClusters();

        Key key = uniqueKey("list");
        List<Integer> list = Arrays.asList(9, 1, 2, 8, 3, 7, 4, 6, 5);
        List<Value> intList = new ArrayList<>();
        for (Integer i : list) {
            intList.add(Value.get(i));
        }

        getClient(TWO_CLUSTER_A).operate(null, key,
                ListOperation.create("ordered", ListOrder.ORDERED, false),
                ListOperation.create("unordered", ListOrder.UNORDERED, false),
                ListOperation.clear("ordered"),
                ListOperation.clear("unordered"),
                ListOperation.appendItems("ordered", intList),
                ListOperation.appendItems("unordered", intList));

        getClient(TWO_CLUSTER_B).operate(null, key,
                ListOperation.create("ordered", ListOrder.ORDERED, false),
                ListOperation.create("unordered", ListOrder.UNORDERED, false),
                ListOperation.clear("ordered"),
                ListOperation.clear("unordered"),
                ListOperation.appendItems("ordered", intList),
                ListOperation.appendItems("unordered", intList),
                ListOperation.sort("unordered", ListSortFlags.DEFAULT));

        assertBinValuesDiffer(key, TWO_CLUSTER_A, TWO_CLUSTER_B, "unordered");

        DifferenceSummary differences = runComparator(twoClusterScanArgs("RECORD_DIFFERENCES", null));
        assertTrue(differences.areDifferent());

        ClusterComparatorOptions options = new ClusterComparatorOptions(
                twoClusterScanArgs("RECORD_DIFFERENCES", null));
        options.setPathOptions(new PathOptions(
                new PathOption("/" + NAMESPACE + "/" + getTestSetName() + "/unordered", PathAction.IGNORE)));
        differences = new ClusterComparator(options).begin();
        assertFalse(differences.areDifferent());

        options.setPathOptions(new PathOptions(
                new PathOption("/" + NAMESPACE + "/" + getTestSetName() + "/unordered", PathAction.COMPAREUNORDERED)));
        differences = new ClusterComparator(options).begin();
        assertFalse(differences.areDifferent());
    }
}
