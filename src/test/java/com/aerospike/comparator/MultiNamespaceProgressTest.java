package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;

import org.junit.jupiter.api.Test;

import com.aerospike.comparator.dbaccess.AerospikeClientAccess;
import com.aerospike.comparator.web.ProgressSnapshot;

public class MultiNamespaceProgressTest {

    private ClusterComparator comparator(String namespaces, String sets) throws Exception {
        String[] args = sets == null
                ? new String[] {
                        "--hosts1", "h1:3000",
                        "--hosts2", "h2:3000",
                        "--namespaces", namespaces,
                        "--action", "scan",
                        "--quiet"
                }
                : new String[] {
                        "--hosts1", "h1:3000",
                        "--hosts2", "h2:3000",
                        "--namespaces", namespaces,
                        "--setNames", sets,
                        "--action", "scan",
                        "--quiet"
                };
        return new ClusterComparator(new ClusterComparatorOptions(args));
    }

    @Test
    void formatProgressScope_includesNamespaceOrdinalWhenMultipleNamespaces() throws Exception {
        ClusterComparator comparator = comparator("test1,test2,test3,test4,test5", null);
        comparator.startComparisonUnit("test2", null, 2, 5, 0, 0);
        assertEquals("Namespace test2 [2/5]", comparator.formatProgressScope());
        assertTrue(comparator.hasMultipleScanUnits());
    }

    @Test
    void formatProgressScope_includesSetOrdinalWhenMultipleSets() throws Exception {
        ClusterComparator comparator = comparator("test", "users,accounts");
        comparator.startComparisonUnit("test", "accounts", 1, 1, 2, 2);
        assertEquals("Namespace test, set accounts [2/2]", comparator.formatProgressScope());
    }

    @Test
    void formatProgressScope_isEmptyForSingleNamespace() throws Exception {
        ClusterComparator comparator = comparator("test", null);
        comparator.startComparisonUnit("test", null, 1, 1, 0, 0);
        assertEquals("", comparator.formatProgressScope());
        assertFalse(comparator.hasMultipleScanUnits());
    }

    @Test
    void startComparisonUnit_resetsPartitionCompletionFlags() throws Exception {
        ClusterComparator comparator = comparator("a,b", null);
        comparator.startComparisonUnit("a", null, 1, 2, 0, 0);
        comparator.partitionsComplete[0].set(true);
        comparator.partitionsComplete[comparator.partitionsComplete.length - 1].set(true);
        assertEquals("[0,4095]", comparator.getPartitionsComplete());

        comparator.startComparisonUnit("b", null, 2, 2, 0, 0);
        assertEquals("[]", comparator.getPartitionsComplete());
        assertFalse(comparator.partitionsComplete[0].get());
        assertEquals("Namespace b [2/2]", comparator.formatProgressScope());
    }

    @Test
    void progressSnapshot_countsOnlyPartitionsInThePartitionList() throws Exception {
        ClusterComparator comparator = new ClusterComparator(new ClusterComparatorOptions(new String[] {
                "--hosts1", "h1:3000",
                "--hosts2", "h2:3000",
                "--namespaces", "test",
                "--partitionList", "4,5,9"
        }));
        comparator.partitionsComplete[5].set(true);
        comparator.partitionsComplete[0].set(true);

        ProgressSnapshot snapshot = comparator.getProgressSnapshot();
        assertEquals(3, snapshot.getTotalPartitions());
        assertEquals(1, snapshot.getPartitionsComplete());
    }

    @Test
    void terminatedRunDoesNotStartLaterNamespaces() throws Exception {
        ClusterComparator comparator = comparator("test1,test2", null);
        comparator.forceTerminate = true;
        Method performComparisons = ClusterComparator.class.getDeclaredMethod(
                "performComparisons", AerospikeClientAccess[].class);
        performComparisons.setAccessible(true);
        performComparisons.invoke(comparator, (Object) new AerospikeClientAccess[] {null, null});
        assertNull(comparator.getProgressSnapshot().getCurrentNamespace());
    }
}
