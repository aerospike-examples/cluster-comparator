package com.aerospike.comparator;

import com.aerospike.client.policy.AuthMode;

public class ClusterConfig {
    private String hostName;
    private String clusterName;
    private String userName;
    private String password;
    private AuthMode authMode = AuthMode.INTERNAL;
    private TlsOptions tls;
    private boolean useServicesAlternate;
    public String getHostName() {
        return hostName;
    }
    public void setHostName(String hostName) {
        this.hostName = hostName;
    }
    public String getClusterName() {
        return clusterName;
    }
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }
    public String getUserName() {
        return userName;
    }
    public void setUserName(String userName) {
        this.userName = userName;
    }
    public String getPassword() {
        return password;
    }
    public void setPassword(String password) {
        this.password = password;
    }
    public AuthMode getAuthMode() {
        return authMode;
    }
    public void setAuthMode(AuthMode authMode) {
        this.authMode = authMode;
    }
    public TlsOptions getTls() {
        return tls;
    }
    public void setTls(TlsOptions tls) {
        this.tls = tls;
    }
    public boolean isUseServicesAlternate() {
        return useServicesAlternate;
    }
    public void setUseServicesAlternate(boolean useServicesAlternate) {
        this.useServicesAlternate = useServicesAlternate;
    }
    @Override
    public String toString() {
        return String.format("name:%s, host:%s, user:%s", this.clusterName, this.hostName, this.userName);
    }
}
