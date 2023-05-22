package com.aerospike.comparator;

import java.io.IOException;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.command.Buffer;

public class ConsoleDifferenceHandler implements MissingRecordHandler, RecordDifferenceHandler {
    
    @Override
    public void handle(int partitionId, Key key, boolean missingFromSide1) throws IOException {
        String side1Digest = missingFromSide1 ? "" : Buffer.bytesToHexString(key.digest);
        String side2Digest = missingFromSide1 ? Buffer.bytesToHexString(key.digest) : "";
        System.out.printf("%s,%s,%s,%s\n",key.namespace,key.setName,side1Digest, side2Digest);
    }

    @Override
    public void handle(int partitionId, Key key, Record side1, Record side2, DifferenceSet differences)
            throws IOException {
        
        if (differences.isQuickCompare()) {
            System.out.printf("DIFFERENCES: %s,%s,%s\n", key.namespace, key.setName, Buffer.bytesToHexString(key.digest));
        }
        else {
            System.out.printf("DIFFERENCES: %s,%s,%s,%s\n", key.namespace, key.setName, Buffer.bytesToHexString(key.digest), differences.toString());
        }
    }
}
