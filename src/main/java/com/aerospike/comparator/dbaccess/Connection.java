package com.aerospike.comparator.dbaccess;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.TlsPolicy;

class Connection {
    private final Socket socket;
    private final DataInputStream dis;
    private final DataOutputStream dos;
    
    public Connection(String host, int port, TlsPolicy tlsPolicy) throws IOException {
        if (tlsPolicy == null) {
            this.socket = new Socket(host, port);
        }
        else if (tlsPolicy.context == null) {
            throw new AerospikeException("Remote connection has a TLS Policy specified but it has no SSL context on it.");
        }
        else {
            this.socket = tlsPolicy.context.getSocketFactory().createSocket(host, port);
        }
        this.dis = new DataInputStream(socket.getInputStream());
        this.dos = new DataOutputStream(socket.getOutputStream());
    }
    
    public void close() {
        try {
            this.dis.close();
            this.dos.close();
            this.socket.close();
        }
        catch (IOException ignored) {}
    }
    
    public DataInputStream getDis() {
        return dis;
    }
    public DataOutputStream getDos() {
        return dos;
    }
}