package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import org.junit.jupiter.api.Assertions;
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

public class AerospikeComparatorTest {
    @Test
    public void testTypes() {
        AerospikeComparator comparator = new AerospikeComparator();
        assertTrue(comparator.compare(null, null) == 0);
        assertTrue(comparator.compare(null, true) < 0);
        assertTrue(comparator.compare(null, false) < 0);
        assertTrue(comparator.compare(true, null) > 0);
        assertTrue(comparator.compare(false, null) > 0);
        assertTrue(comparator.compare(false, false) == 0);
        assertTrue(comparator.compare(false, true) < 0);
    }
}
