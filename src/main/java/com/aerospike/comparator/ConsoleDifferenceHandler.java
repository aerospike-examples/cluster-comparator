package com.aerospike.comparator;

import java.io.IOException;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.command.Buffer;

public class ConsoleDifferenceHandler implements MissingRecordHandler, RecordDifferenceHandler {
    
    @Override
    public void handle(int partitionId, Key key, Side missingFromSide) throws IOException {
        String side1Digest = missingFromSide == Side.SIDE_1 ? "" : Buffer.bytesToHexString(key.digest);
        String side2Digest = missingFromSide == Side.SIDE_2 ? "" : Buffer.bytesToHexString(key.digest);
        System.out.printf("%s,%s,%s,%s,%s\n",key.namespace,key.setName, key.userKey, side1Digest, side2Digest);
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
