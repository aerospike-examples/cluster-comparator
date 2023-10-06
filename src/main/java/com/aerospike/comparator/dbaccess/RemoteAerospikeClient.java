package com.aerospike.comparator.dbaccess;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.Statement;
import com.aerospike.comparator.ClusterComparatorOptions;
import com.aerospike.comparator.ClusterComparatorOptions.CompareMode;

public class RemoteAerospikeClient implements AerospikeClientAccess {

    private final ConnectionPool pool;
    private final int cacheSize;
    private final boolean useHashes;
    private final CompareMode compareMode;
    
    public RemoteAerospikeClient(String host, int port, int defaultPoolSize, TlsPolicy tlsPolicy, ClusterComparatorOptions options) throws IOException {
        this.pool = new ConnectionPool(host, port, defaultPoolSize, tlsPolicy);
        this.cacheSize = options.getRemoteCacheSize();
        this.useHashes = options.isRemoteServerHashes();
        this.compareMode = options.getCompareMode();
        
        sendOptionsToServer(options);
    }
    
    private void sendOptionsToServer(ClusterComparatorOptions options) {
        Connection conn = null;
        try {
            conn = this.pool.borrow();
            conn.getDos().write(RemoteServer.CMD_CONFIG);
            conn.getDos().writeBoolean(options.isSortMaps());
            conn.getDis().readInt();
        }
        catch (IOException ioe) {
            throw new AerospikeException(ioe);
        }
        finally {
            if (conn != null) {
                this.pool.release(conn);
            }
        }
    }
    
    @Override
    public void close() {
        Connection conn = null;
        try {
            conn = this.pool.borrow();
            conn.getDos().write(RemoteServer.CMD_CLOSE);
            conn.getDis().readInt();
        }
        catch (IOException ioe) {
            throw new AerospikeException(ioe);
        }
        finally {
            if (conn != null) {
                this.pool.release(conn);
            }
            this.pool.close();
        }
    }

    @Override
    public void touch(WritePolicy policy, Key key) {
        Connection conn = null;
        try {
            conn = this.pool.borrow();
            conn.getDos().write(RemoteServer.CMD_TOUCH);
            RemoteUtils.sendPolicy(policy, conn.getDos());
            RemoteUtils.sendKey(key, conn.getDos());
            conn.getDis().readInt();
        }
        catch (IOException ioe) {
            RemoteUtils.handleIOException(ioe);
            throw new AerospikeException(ioe);
        }
        finally {
            if (conn != null) {
                this.pool.release(conn);
            }
        }
    }

    @Override
    public Record get(Policy policy, Key key) {
        Connection conn = null;
        try {
            conn = this.pool.borrow();
            conn.getDos().write(RemoteServer.CMD_GET);
            RemoteUtils.sendPolicy(policy, conn.getDos());
            RemoteUtils.sendKey(key, conn.getDos());
            return RemoteUtils.readRecord(conn.getDis());
        }
        catch (IOException ioe) {
            RemoteUtils.handleIOException(ioe);
            throw new AerospikeException(ioe);
        }
        finally {
            if (conn != null) {
                this.pool.release(conn);
            }
        }
    }

    @Override
    public RecordSetAccess queryPartitions(QueryPolicy queryPolicy, Statement statement, PartitionFilter filter) {
        Connection conn = null;
        try {
            conn = this.pool.borrow();
            conn.getDos().write(RemoteServer.CMD_QUERY_PARTITION);
            conn.getDos().writeInt(queryPolicy.maxConcurrentNodes);
            conn.getDos().writeBoolean(queryPolicy.includeBinData);
            conn.getDos().writeBoolean(queryPolicy.shortQuery);
            if (queryPolicy.filterExp == null) {
                conn.getDos().writeInt(0);
            }
            else {
                byte[] bytes = queryPolicy.filterExp.getBytes();
                conn.getDos().writeInt(bytes.length);
                conn.getDos().write(bytes);
            }
            conn.getDos().writeUTF(statement.getNamespace());
            conn.getDos().writeUTF(statement.getSetName());
            conn.getDos().writeInt(statement.getRecordsPerSecond());
            conn.getDos().writeInt(filter.getBegin());
            conn.getDos().writeInt(filter.getCount());
            conn.getDis().readUTF();    // Getting this back means the server is ready.
            // We keep hold of this connection until the recordset is closed, which simplifies the back-and-forth
            return new RemoteRecordSet(pool, conn, this.cacheSize, useHashes, compareMode);
        }
        catch (IOException ioe) {
            RemoteUtils.handleIOException(ioe);
            throw new AerospikeException(ioe);
        }
    }

    @Override
    public Map<String, String> invokeInfoCommandOnAllNodes(String info) {
        Connection conn = null;
        try {
            conn = this.pool.borrow();
            conn.getDos().write(RemoteServer.CMD_INVOKE_INFO_CMD_ON_ALL_NODES);
            conn.getDos().writeUTF(info);
            int size = conn.getDis().readInt();
            Map<String, String> result = new HashMap<>();
            for (int i = 0; i < size; i++) {
                String key = conn.getDis().readUTF();
                String value = conn.getDis().readUTF();
                result.put(key, value);
            }
            return result;
        }
        catch (IOException ioe) {
            RemoteUtils.handleIOException(ioe);
            throw new AerospikeException(ioe);
        }
        finally {
            if (conn != null) {
                this.pool.release(conn);
            }
        }
    }

    @Override
    public String invokeInfoCommandOnANode(String info) {
        Connection conn = null;
        try {
            conn = this.pool.borrow();
            conn.getDos().write(RemoteServer.CMD_INVOKE_INFO_CMD_ON_A_NODE);
            conn.getDos().writeUTF(info);
            return conn.getDis().readUTF();
        }
        catch (IOException ioe) {
            RemoteUtils.handleIOException(ioe);
            throw new AerospikeException(ioe);
        }
        finally {
            if (conn != null) {
                this.pool.release(conn);
            }
        }
    }

    @Override
    public List<String> getNodeNames() {
        Connection conn = null;
        try {
            conn = this.pool.borrow();
            conn.getDos().write(RemoteServer.CMD_GET_NODE_NAMES);
            int size = conn.getDis().readInt();
            List<String> result = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                result.add(conn.getDis().readUTF());
            }
            return result;
        }
        catch (IOException ioe) {
            RemoteUtils.handleIOException(ioe);
            throw new AerospikeException(ioe);
        }
        finally {
            if (conn != null) {
                this.pool.release(conn);
            }
        }
    }

    @Override
    public boolean isLocal() {
        return false;
    }

}
