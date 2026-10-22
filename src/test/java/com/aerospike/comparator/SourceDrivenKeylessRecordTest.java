package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.Statement;
import com.aerospike.comparator.dbaccess.AerospikeClientAccess;
import com.aerospike.comparator.dbaccess.RecordMetadata;
import com.aerospike.comparator.dbaccess.RecordSetAccess;

/**
 * Set-mapping scans skip records that have no stored user key. Those records must be counted as
 * skipped and must not also be reported as missing on the mapped set.
 */
public class SourceDrivenKeylessRecordTest {

    @Test
    void keylessRecordsAreSkippedAndNotReportedMissing() throws Exception {
        Path config = Files.createTempFile("set-mapping-", ".yaml");
        Files.writeString(config, ""
                + "clusters:\n"
                + "  - hostName: h1:3000\n"
                + "  - hostName: h2:3000\n"
                + "setMapping:\n"
                + "- set: nokey_app\n"
                + "  mappings:\n"
                + "  - clusterIndex: 2\n"
                + "    name: nokey_target\n");
        config.toFile().deleteOnExit();

        ClusterComparator comparator = new ClusterComparator(new ClusterComparatorOptions(new String[] {
                "--configFile", config.toString(),
                "--sourceCluster", "1",
                "--namespaces", "test",
                "--setNames", "nokey_app",
                "--quiet"
        }));
        Field threads = ClusterComparator.class.getDeclaredField("threadsToUse");
        threads.setAccessible(true);
        threads.setInt(comparator, 1);

        Key keyed = new Key("test", "nokey_app", "user-1");
        Key keylessA = keyless("a");
        Key keylessB = keyless("b");
        assertNull(keylessA.userKey);

        ScanClient source = new ScanClient(keyed, keylessA, keylessB);
        LookupClient destination = new LookupClient();

        new SourceDrivenPartitionComparator(comparator).comparePartition(
                new AerospikeClientAccess[] {source, destination}, "test", "nokey_app", 0);

        assertEquals(2, comparator.getRecordsSkippedMissingUserKey());
        assertEquals(1, comparator.getProgressSnapshot().getTotalMissingRecords());
        assertEquals(0, comparator.getProgressSnapshot().getRecordsMissingPerCluster()[0]);
        assertEquals(1, comparator.getProgressSnapshot().getRecordsMissingPerCluster()[1]);
        assertEquals(1, destination.lookups.size());
        assertEquals(1, destination.lookups.get(0).length);
        assertEquals("user-1", destination.lookups.get(0)[0].userKey.getObject());
        assertEquals("nokey_target", destination.lookups.get(0)[0].setName);
        assertTrue(comparator.partitionsComplete[0].get());
    }

    private static Key keyless(String userKey) {
        Key withKey = new Key("test", "nokey_app", userKey);
        return new Key("test", withKey.digest, "nokey_app", null);
    }

    private static final class ListRecordSet implements RecordSetAccess {
        private final Key[] keys;
        private int index = -1;

        ListRecordSet(Key[] keys) {
            this.keys = keys;
        }

        @Override
        public boolean next() {
            return ++index < keys.length;
        }

        @Override
        public Key getKey() {
            return keys[index];
        }

        @Override
        public Record getRecord() {
            return null;
        }

        @Override
        public byte[] getRecordHash(boolean sortMaps) {
            return null;
        }

        @Override
        public void close() {
        }
    }

    private static final class ScanClient implements AerospikeClientAccess {
        private final Key[] keys;

        ScanClient(Key... keys) {
            this.keys = keys;
        }

        @Override
        public RecordSetAccess queryPartitions(QueryPolicy queryPolicy, Statement statement, PartitionFilter filter) {
            return new ListRecordSet(keys);
        }

        @Override
        public boolean isLocal() {
            return true;
        }

        @Override
        public void close() {
        }

        @Override
        public void put(WritePolicy policy, Key key, Bin... bins) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void touch(WritePolicy policy, Key key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete(WritePolicy policy, Key key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists(Policy policy, Key key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Record get(Policy policy, Key key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean[] exists(BatchPolicy policy, Key[] keys) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Record[] get(BatchPolicy policy, Key[] keys) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RecordMetadata getMetadata(WritePolicy policy, Key key) {
            return null;
        }

        @Override
        public Map<String, String> invokeInfoCommandOnAllNodes(String info) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String invokeInfoCommandOnANode(String info) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> getNodeNames() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class LookupClient implements AerospikeClientAccess {
        private final List<Key[]> lookups = new ArrayList<>();

        @Override
        public boolean[] exists(BatchPolicy policy, Key[] keys) {
            lookups.add(keys);
            return new boolean[keys.length];
        }

        @Override
        public boolean isLocal() {
            return true;
        }

        @Override
        public void close() {
        }

        @Override
        public void put(WritePolicy policy, Key key, Bin... bins) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void touch(WritePolicy policy, Key key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete(WritePolicy policy, Key key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists(Policy policy, Key key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Record get(Policy policy, Key key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Record[] get(BatchPolicy policy, Key[] keys) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RecordMetadata getMetadata(WritePolicy policy, Key key) {
            return null;
        }

        @Override
        public RecordSetAccess queryPartitions(QueryPolicy queryPolicy, Statement statement, PartitionFilter filter) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> invokeInfoCommandOnAllNodes(String info) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String invokeInfoCommandOnANode(String info) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> getNodeNames() {
            throw new UnsupportedOperationException();
        }
    }
}
