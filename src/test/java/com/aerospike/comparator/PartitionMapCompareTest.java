package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.aerospike.comparator.dbaccess.StubAerospikeClient;

public class PartitionMapCompareTest {

    @Test
    void compareUsesEachSidesNamespaceName() {
        PartitionMap left = map("n1", "prod", 5, 10);
        PartitionMap right = map("n1", "stage", 5, 4);

        assertEquals(List.of(5), left.compare(right, "prod", "stage"));
        assertThrows(QuickCompareException.class, () -> left.compare(right, "prod"));
    }

    @Test
    void compareWithOneNamespaceNameStillWorksWhenBothSidesUseIt() {
        PartitionMap left = map("n1", "prod", 5, 10);
        PartitionMap right = map("n1", "prod", 5, 10);
        assertEquals(Collections.emptyList(), left.compare(right, "prod"));
    }

    private static PartitionMap map(String node, String namespace, int differentPartition, long recordsThere) {
        String header = "namespace:partition:working_master:records:tombstones:emigrates:immigrates";
        StringBuilder body = new StringBuilder(header).append(';');
        for (int partition = 0; partition < 4096; partition++) {
            long records = partition == differentPartition ? recordsThere : 1;
            body.append(namespace).append(':')
                    .append(partition).append(':')
                    .append(node).append(':')
                    .append(records).append(":0:0:0;");
        }
        String info = body.toString();
        return new PartitionMap(new StubAerospikeClient() {
            @Override
            public Map<String, String> invokeInfoCommandOnAllNodes(String command) {
                return Map.of(node, info);
            }
        });
    }
}
