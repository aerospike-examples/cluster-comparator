package com.aerospike.comparator.dbaccess;

import java.io.IOException;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

public class RemoteRecordSet implements RecordSetAccess {
    private final ConnectionPool pool;
    private final Connection connection;
    private final CachedRecordSet cachedRecordSet;
    
    public RemoteRecordSet(ConnectionPool pool, Connection connection, int cacheSize) {
        super();
        this.pool = pool;
        this.connection = connection;
        if (cacheSize >= 4) {
            this.cachedRecordSet = new CachedRecordSet(cacheSize, connection);
        }
        else {
            this.cachedRecordSet = null;
        }
    }

    @Override
    public boolean next() {
        if (cachedRecordSet != null) {
            return cachedRecordSet.next();
        }
        try {
            connection.getDos().write(RemoteServer.CMD_RS_NEXT);
            return connection.getDis().readBoolean();
        }
        catch (IOException ioe) {
            throw new AerospikeException(ioe);
        }
    }

    @Override
    public Key getKey() {
        if (cachedRecordSet != null) {
            return cachedRecordSet.getKey();
        }
        try {
            connection.getDos().write(RemoteServer.CMD_RS_KEY);
            return RemoteUtils.readKey(connection.getDis());
        }
        catch (IOException ioe) {
            throw new AerospikeException(ioe);
        }
    }

    @Override
    public Record getRecord() {
        if (cachedRecordSet != null) {
            return cachedRecordSet.getRecord();
        }
        try {
            connection.getDos().write(RemoteServer.CMD_RS_RECORD);
            return RemoteUtils.readRecord(connection.getDis());
        }
        catch (IOException ioe) {
            throw new AerospikeException(ioe);
        }
    }

    @Override
    public void close() {
        try {
            if (cachedRecordSet != null) {
                cachedRecordSet.close();
            }
            else {
                connection.getDos().write(RemoteServer.CMD_RS_CLOSE);
                connection.getDis().readInt();
            }
        }
        catch (IOException ioe) {
            throw new AerospikeException(ioe);
        }
        finally {
            pool.release(connection);
            
        }
    }
}
