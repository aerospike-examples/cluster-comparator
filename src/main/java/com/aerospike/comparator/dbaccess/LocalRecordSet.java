package com.aerospike.comparator.dbaccess;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.util.Crypto;

import gnu.crypto.hash.RipeMD160;

public class LocalRecordSet implements RecordSetAccess {
    private final RecordSet recordSet;
    
    public LocalRecordSet(RecordSet recordSet) {
        super();
        this.recordSet = recordSet;
    }

    @Override
    public boolean next() {
        return this.recordSet.next();
    }

    @Override
    public Key getKey() {
        return this.recordSet.getKey();
    }

    @Override
    public Record getRecord() {
        return this.recordSet.getRecord();
    }

    @Override
    public void close() {
        this.recordSet.close();
    }
    
    @Override
    public byte[] getRecordHash(boolean sortMaps) {
        return RemoteUtils.getRecordHash(this.getRecord(), sortMaps);
    }
}
