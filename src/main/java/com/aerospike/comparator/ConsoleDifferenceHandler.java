package com.aerospike.comparator;

import java.io.IOException;
import java.util.List;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.command.Buffer;

public class ConsoleDifferenceHandler implements MissingRecordHandler, RecordDifferenceHandler {
    
    @Override
    public void handle(int partitionId, Key key, List<Integer> missingFromClusters) throws IOException {
        System.out.printf("%s,%s,%s,%s,%s\n",key.namespace,key.setName, key.userKey, missingFromClusters);
    }

    @Override
    public void handle(int partitionId, Key key, Record side1, Record side2, DifferenceSet differences)
            throws IOException {
        
        if (differences.isQuickCompare()) {
            System.out.printf("DIFFERENCES: %s,%s,%s,%s\n", key.namespace, key.setName, key.userKey, Buffer.bytesToHexString(key.digest));
        }
        else {
            System.out.printf("DIFFERENCES: %s,%s,%s,%s,%s\n", key.namespace, key.setName, key.userKey, Buffer.bytesToHexString(key.digest), differences.toString());
        }
    }
}
