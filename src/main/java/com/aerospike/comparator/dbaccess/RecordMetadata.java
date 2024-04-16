package com.aerospike.comparator.dbaccess;

import java.io.Serializable;

import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;

public class RecordMetadata implements Serializable {
    private long deviceSize;
    private long memorySize;
    private long recordSize;
    private boolean keyExists;
    private long lastUpdateMs;
    private long sinceUpdateMs;
    private String setName;
    private long ttl;
    private long voidTimeMs;
    private int generation;
    private int expiration;
    
    private static final Operation[] metadataOps = new Operation[] {
            ExpOperation.read("deviceSize", Exp.build(Exp.deviceSize()), ExpReadFlags.EVAL_NO_FAIL),
            ExpOperation.read("memorySize", Exp.build(Exp.memorySize()), ExpReadFlags.EVAL_NO_FAIL),
//            ExpOperation.read("recordSize", Exp.build(Exp.recordSize()), ExpReadFlags.EVAL_NO_FAIL),
            ExpOperation.read("keyExists", Exp.build(Exp.keyExists()), ExpReadFlags.EVAL_NO_FAIL),
            ExpOperation.read("lastUpdate", Exp.build(Exp.lastUpdate()), ExpReadFlags.EVAL_NO_FAIL),
            ExpOperation.read("sinceUpdate", Exp.build(Exp.sinceUpdate()), ExpReadFlags.EVAL_NO_FAIL),
            ExpOperation.read("setName", Exp.build(Exp.setName()), ExpReadFlags.EVAL_NO_FAIL),
            ExpOperation.read("ttl", Exp.build(Exp.ttl()), ExpReadFlags.EVAL_NO_FAIL),
            ExpOperation.read("voidTime", Exp.build(Exp.voidTime()), ExpReadFlags.EVAL_NO_FAIL)
    };
    
    public static Operation[] getMetadataOperations() {
        return metadataOps;
    }
            
    public RecordMetadata() {}
            
    public RecordMetadata(Record record) {
        this.deviceSize = record.getLong("deviceSize");
        this.memorySize = record.getLong("memorySize");
//        this.recordSize = record.getLong("recordSize");
        this.recordSize = Math.max(this.deviceSize,  this.memorySize);
        this.keyExists = record.getBoolean("keyExists");
        this.lastUpdateMs = record.getLong("lastUpdate") / 1_000_000;
        this.sinceUpdateMs = record.getLong("sinceUpdate");
        this.setName = record.getString("setName");
        this.ttl = record.getLong("ttl");
        this.voidTimeMs = record.getLong("voidTime") / 1_000_000;
        this.generation = record.generation;
        this.expiration = record.expiration;
    }


    public long getDeviceSize() {
        return deviceSize;
    }

    public long getMemorySize() {
        return memorySize;
    }

    public long getRecordSize() {
        return recordSize;
    }

    public boolean keyExists() {
        return keyExists;
    }

    public long getLastUpdateMs() {
        return lastUpdateMs;
    }

    public long getSinceUpdateMs() {
        return sinceUpdateMs;
    }

    public String getSetName() {
        return setName;
    }

    public long getTtl() {
        return ttl;
    }

    public long getVoidTimeMs() {
        return voidTimeMs;
    }
    
    public int getExpiration() {
        return expiration;
    }
    
    public int getGeneration() {
        return generation;
    }

    
    public void setDeviceSize(long deviceSize) {
        this.deviceSize = deviceSize;
    }

    public void setMemorySize(long memorySize) {
        this.memorySize = memorySize;
    }

    public void setRecordSize(long recordSize) {
        this.recordSize = recordSize;
    }

    public void setKeyExists(boolean keyExists) {
        this.keyExists = keyExists;
    }

    public void setLastUpdateMs(long lastUpdateMs) {
        this.lastUpdateMs = lastUpdateMs;
    }

    public void setSinceUpdateMs(long sinceUpdateMs) {
        this.sinceUpdateMs = sinceUpdateMs;
    }

    public void setSetName(String setName) {
        this.setName = setName;
    }

    public void setTtl(long ttl) {
        this.ttl = ttl;
    }

    public void setVoidTimeMs(long voidTime) {
        this.voidTimeMs = voidTime;
    }
    
    public void setExpiration(int expiration) {
        this.expiration = expiration;
    }
    
    public void setGeneration(int generation) {
        this.generation = generation;
    }

    public static Operation[] getMetadataops() {
        return metadataOps;
    }
}
