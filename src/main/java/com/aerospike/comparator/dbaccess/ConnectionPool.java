package com.aerospike.comparator.dbaccess;

import java.io.IOException;
import java.security.cert.CertificateParsingException;
import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.TlsPolicy;

class ConnectionPool {
    private final List<Connection> available = new ArrayList<>();
    private final List<Connection> inUse = new ArrayList<>();
    private final String host;
    private final int port;
    private final TlsPolicy tlsPolicy;
    private volatile boolean closed = false;
    
    public ConnectionPool(String host, int port, int defaultSize, TlsPolicy tlsPolicy) throws IOException {
        this.host = host;
        this.port = port;
        this.tlsPolicy = tlsPolicy;
        for (int i = 0; i < defaultSize; i++) {
            Connection connection = establish();
            available.add(connection);
        }
    }
    private Connection establish() throws IOException {
        try {
            return new Connection(host, port, tlsPolicy);
        }
        catch (CertificateParsingException cpe) {
            cpe.printStackTrace();
            throw new AerospikeException(cpe);
        }
    }
    
    public synchronized Connection borrow() throws IOException {
        if (closed) {
            throw new AerospikeException("Cannot borrow a connection when the pool is closed");
        }
        Connection conn;
        if (available.size() > 0) {
            conn = available.remove(0);
        }
        else {
            conn = establish();
        }
        inUse.add(conn);
        return conn;
    }
    
    public synchronized void release(Connection conn) {
        if (closed) {
            conn.close();
        }
        else if (inUse.remove(conn)) {
            available.add(conn);
        }
    }
    
    public synchronized void close() {
        this.closed = true;
        for (Connection conn : this.available) {
            conn.close();
        }
        this.available.clear();
    }
}