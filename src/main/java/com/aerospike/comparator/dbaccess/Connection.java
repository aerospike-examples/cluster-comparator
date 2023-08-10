package com.aerospike.comparator.dbaccess;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.x500.X500Principal;

import com.aerospike.client.policy.TlsPolicy;

class Connection {
    private final Socket socket;
    private final DataInputStream dis;
    private final DataOutputStream dos;
    
    public Connection(String host, int port, TlsPolicy tlsPolicy) throws IOException, CertificateParsingException {
        if (tlsPolicy == null) {
            this.socket = new Socket(host, port);
        }
//        else if (tlsPolicy.context == null) {
//            throw new AerospikeException("Remote connection has a TLS Policy specified but it has no SSL context on it.");
//        }
        else {
            SSLSocketFactory sslsocketfactory = (tlsPolicy.context != null) ?
                    tlsPolicy.context.getSocketFactory() :
                    (SSLSocketFactory)SSLSocketFactory.getDefault();
            SSLSocket sslSocket = (SSLSocket)sslsocketfactory.createSocket();
            if (tlsPolicy.protocols != null) {
                sslSocket.setEnabledProtocols(tlsPolicy.protocols);
            }
            if (tlsPolicy.ciphers != null) {
                sslSocket.setEnabledCipherSuites(tlsPolicy.ciphers);
            }
            sslSocket.setUseClientMode(true);
            sslSocket.connect(new InetSocketAddress(host, port));
            sslSocket.startHandshake();
            
            System.out.println("\tCertificate details:");
            X509Certificate cert = (X509Certificate)sslSocket.getSession().getPeerCertificates()[0];
            String subject = cert.getSubjectX500Principal().getName(X500Principal.RFC2253);
            Collection<List<?>> allNames = cert.getSubjectAlternativeNames();
            System.out.printf("\t\tPeer certificate subject: %s\n", subject);
            System.out.println("\t\tSubject AlternativeNames:");
            if (allNames != null) {
                for (List<?> list : allNames) {
                    int type = (Integer)list.get(0);
                    System.out.printf("\t\t\tSAN %d = %s\n", type, list.get(1));
                }
            }
            System.out.printf( "\t\tProtocol: %s\n", sslSocket.getSession().getProtocol());
            System.out.printf( "\t\tCipher: %s\n", sslSocket.getSession().getCipherSuite());
            this.socket = sslSocket;

//            this.socket = tlsPolicy.context.getSocketFactory().createSocket(host, port);
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