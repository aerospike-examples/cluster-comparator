package com.aerospike.comparator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

import com.aerospike.client.AerospikeException;

import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.pem.util.PemUtils;

public class SSLOptions {
    private String certChain;
    private String privateKey;
    private String caCertChain;
    private String keyPassword;
    public String getCertChain() {
        return certChain;
    }
    public void setCertChain(String certChain) {
        this.certChain = certChain;
    }
    public String getPrivateKey() {
        return privateKey;
    }
    public void setPrivateKey(String privateKey) {
        this.privateKey = privateKey;
    }
    public String getCaCertChain() {
        return caCertChain;
    }
    public void setCaCertChain(String caCertChain) {
        this.caCertChain = caCertChain;
    }
    public String getKeyPassword() {
        return keyPassword;
    }
    public void setKeyPassword(String keyPassword) {
        this.keyPassword = keyPassword;
    }
    
    public SSLFactory toSSLFactory() {
        InputStream certFile = null;
        InputStream keyFile = null;
        InputStream caFile = null;
        try {
            try {
                certFile = new FileInputStream(new File(certChain));
            } catch (FileNotFoundException e) {
                throw new AerospikeException(String.format("certChain file '%s' not found", certChain));
            }
            try {
                keyFile = new FileInputStream(new File(privateKey));
            } catch (FileNotFoundException e) {
                throw new AerospikeException(String.format("privateKey file '%s' not found", certChain));
            }
            try {
                caFile = new FileInputStream(new File(caCertChain));
            } catch (FileNotFoundException e) {
                throw new AerospikeException(String.format("caCertChain file '%s' not found", certChain));
            }
            X509ExtendedKeyManager keyManager = PemUtils.loadIdentityMaterial(certFile, keyFile, keyPassword == null ? null : keyPassword.toCharArray());
            X509ExtendedTrustManager trustManager = PemUtils.loadTrustMaterial(caFile);
    
            SSLFactory sslFactory = SSLFactory.builder()
                    .withIdentityMaterial(keyManager)
                    .withTrustMaterial(trustManager)
                    .build();
            return sslFactory;
        }
        finally {
            if (certFile != null) {
                try {
                    certFile.close();
                } catch (IOException ignored) {}
            }
            if (keyFile != null) {
                try {
                    keyFile.close();
                } catch (IOException ignored) {}
            }
            if (caFile != null) {
                try {
                    caFile.close();
                } catch (IOException ignored) {}
            }
        }

    }
}
