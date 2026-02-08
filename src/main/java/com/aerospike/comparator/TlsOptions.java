package com.aerospike.comparator;

import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.util.Util;

public class TlsOptions {
    private String protocols;
    private String ciphers;
    private String revokeCertificates;
    private boolean loginOnly;
    private SSLOptions ssl;
    public String getProtocols() {
        return protocols;
    }
    public void setProtocols(String protocols) {
        this.protocols = protocols;
    }
    public String getCiphers() {
        return ciphers;
    }
    public void setCiphers(String ciphers) {
        this.ciphers = ciphers;
    }
    public String getRevokeCertificates() {
        return revokeCertificates;
    }
    public void setRevokeCertificates(String revokeCerts) {
        this.revokeCertificates = revokeCerts;
    }
    public boolean isLoginOnly() {
        return loginOnly;
    }
    public void setLoginOnly(boolean loginOnly) {
        this.loginOnly = loginOnly;
    }
    public SSLOptions getSsl() {
        return ssl;
    }
    public void setSsl(SSLOptions ssl) {
        this.ssl = ssl;
    }
    
    public TlsPolicy toTlsPolicy(boolean doLogging) {
        TlsPolicy policy = new TlsPolicy();
        policy.protocols = this.protocols != null ? this.protocols.split(",") : null;
        policy.ciphers = this.ciphers != null ? this.ciphers.split(",") : null;
        policy.revokeCertificates = this.revokeCertificates != null ? Util.toBigIntegerArray(this.revokeCertificates) : null;
        policy.forLoginOnly = this.loginOnly;
        policy.context = this.ssl != null ? this.ssl.toSSLFactory(doLogging).getSslContext() : null;
        return policy;
    }
}
