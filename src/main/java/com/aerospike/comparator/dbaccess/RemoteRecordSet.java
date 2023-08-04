package com.aerospike.comparator.dbaccess;

import java.io.IOException;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

public class RemoteRecordSet implements RecordSetAccess {
    private final ConnectionPool pool;
    private final Connection connection;
    
    public RemoteRecordSet(ConnectionPool pool, Connection connection) {
        super();
        this.pool = pool;
        this.connection = connection;
    }

    @Override
    public boolean next() {
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
        try {
            connection.getDos().write(RemoteServer.CMD_RS_KEY);
            String namespace = connection.getDis().readUTF();
            String setName = connection.getDis().readUTF();
            int length = connection.getDis().readInt();
            byte[] digest = connection.getDis().readNBytes(length);
            return new Key(namespace, digest, setName, null);
        }
        catch (IOException ioe) {
            throw new AerospikeException(ioe);
        }
    }

    @Override
    public Record getRecord() {
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
            connection.getDos().write(RemoteServer.CMD_RS_CLOSE);
            connection.getDis().readInt();
            pool.release(connection);
        }
        catch (IOException ioe) {
            throw new AerospikeException(ioe);
        }
    }
}
