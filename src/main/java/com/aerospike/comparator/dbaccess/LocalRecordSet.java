package com.aerospike.comparator.dbaccess;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.query.RecordSet;

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

}
