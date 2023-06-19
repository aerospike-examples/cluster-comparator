package com.aerospike.comparator.metadata;

import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;

public class Set implements Equalish {
    @FieldName("ns") private String namesapce;
    private String set;
    private long objects;
    private long tombstones;
    @FieldName("memory_data_bytes") private long memoryDataBytes;
    @FieldName("device_data_bytes") private long deviceDataBytes;
    @FieldName("truncate_lut") private long truncateLut;
    private long sindexes;
    @FieldName("index_populating") private boolean indexPopulating;
    private boolean truncating;
    @FieldName("disable-eviction") private boolean disableEvictions;
    @FieldName("enable-index") private boolean enableIndex;
    @FieldName("stop-writes-count") private long stopWritesCount;
    @FieldName("stop-writes-size") private long stopWritesSize;

    @Override
    public int hashCode() {
        long code = 3 * (this.namesapce == null ? 0 : this.namesapce.hashCode()) +
                5 * (this.set == null ? 0 : this.set.hashCode()) +
                7 * this.objects +
                11 * this.tombstones +
                13 * this.truncateLut +
                17 * this.memoryDataBytes +
                19 * this.deviceDataBytes + 
                23 * sindexes +
                (this.truncating ? 29 : 0) +
                (this.disableEvictions ? 31 : 0) +
                (this.enableIndex ? 37 : 0) +
                41 * this.stopWritesCount +
                43 * this.stopWritesSize;
        int lowerPart = (int)(code & 0xFFFFFFFF);
        int upperPart = (int)(code >> 32);
        return lowerPart ^ upperPart;
    }
    
    private boolean equals(Object s1, Object s2) {
        return (s1 == null && s2 == null) || (s1 != null && s1.equals(s2));
    }
    
    @Override
    public boolean isEqualish(Object obj) {
        if (this == obj) {
            return true;
        }
        else if (!(obj instanceof Set)) {
            return false;
        }
        Set other = (Set)obj;
        return equals(this.namesapce, other.namesapce) && 
                equals(this.set, other.set) &&
                this.objects == other.objects &&
                this.tombstones == other.tombstones &&
                this.memoryDataBytes == other.memoryDataBytes &&
                this.deviceDataBytes == other.deviceDataBytes &&
                this.sindexes == other.sindexes &&
                this.disableEvictions == other.disableEvictions &&
                this.enableIndex == other.enableIndex &&
                this.stopWritesCount == other.stopWritesCount &&
                this.stopWritesSize == other.stopWritesSize;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        else if (!(obj instanceof Set)) {
            return false;
        }
        Set other = (Set)obj;
        return equals(this.namesapce, other.namesapce) && 
                equals(this.set, other.set) &&
                this.objects == other.objects &&
                this.tombstones == other.tombstones &&
                this.memoryDataBytes == other.memoryDataBytes &&
                this.deviceDataBytes == other.deviceDataBytes &&
                this.truncateLut == other.truncateLut &&
                this.sindexes == other.sindexes &&
                this.indexPopulating == other.indexPopulating &&
                this.truncating == other.truncating &&
                this.disableEvictions == other.disableEvictions &&
                this.enableIndex == other.enableIndex &&
                this.stopWritesCount == other.stopWritesCount &&
                this.stopWritesSize == other.stopWritesSize;
    }

    public String getNamesapce() {
        return namesapce;
    }

    public void setNamesapce(String namesapce) {
        this.namesapce = namesapce;
    }

    public String getSet() {
        return set;
    }

    public void setSet(String set) {
        this.set = set;
    }

    public long getObjects() {
        return objects;
    }

    public void setObjects(long objects) {
        this.objects = objects;
    }

    public long getTombstones() {
        return tombstones;
    }

    public void setTombstones(long tombstones) {
        this.tombstones = tombstones;
    }

    public long getMemoryDataBytes() {
        return memoryDataBytes;
    }

    public void setMemoryDataBytes(long memoryDataBytes) {
        this.memoryDataBytes = memoryDataBytes;
    }

    public long getDeviceDataBytes() {
        return deviceDataBytes;
    }

    public void setDeviceDataBytes(long deviceDataBytes) {
        this.deviceDataBytes = deviceDataBytes;
    }

    public long getTruncateLut() {
        return truncateLut;
    }

    public void setTruncateLut(long truncateLut) {
        this.truncateLut = truncateLut;
    }

    public long getSindexes() {
        return sindexes;
    }

    public void setSindexes(long sindexes) {
        this.sindexes = sindexes;
    }

    public boolean isIndexPopulating() {
        return indexPopulating;
    }

    public void setIndexPopulating(boolean indexPopulating) {
        this.indexPopulating = indexPopulating;
    }

    public boolean isTruncating() {
        return truncating;
    }

    public void setTruncating(boolean truncating) {
        this.truncating = truncating;
    }

    public boolean isDisableEvictions() {
        return disableEvictions;
    }

    public void setDisableEvictions(boolean disableEvictions) {
        this.disableEvictions = disableEvictions;
    }

    public boolean isEnableIndex() {
        return enableIndex;
    }

    public void setEnableIndex(boolean enableIndex) {
        this.enableIndex = enableIndex;
    }

    public long getStopWritesCount() {
        return stopWritesCount;
    }

    public void setStopWritesCount(long stopWritesCount) {
        this.stopWritesCount = stopWritesCount;
    }

    public long getStopWritesSize() {
        return stopWritesSize;
    }

    public void setStopWritesSize(long stopWritesSize) {
        this.stopWritesSize = stopWritesSize;
    }

}
