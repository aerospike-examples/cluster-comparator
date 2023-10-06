package com.aerospike.comparator.dbaccess;

import com.aerospike.client.Key;
import com.aerospike.client.Record;

public interface RecordSetAccess {
    boolean next();
    Key getKey();
    Record getRecord();
    byte[] getRecordHash(boolean sortMaps);
    void close();
}
