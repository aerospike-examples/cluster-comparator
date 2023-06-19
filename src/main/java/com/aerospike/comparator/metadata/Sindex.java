package com.aerospike.comparator.metadata;

import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;

public class Sindex implements Equalish {
    private enum IndexState {
        RO,
        RW
    }
    @FieldName("ns") private String namesapce;
    @FieldName("indexname") private String indexName;
    private String set;
    private String bin;
    @FieldName("type") private IndexType indexType;
    @FieldName("indextype") private IndexCollectionType indexCollectionType;
    private IndexState state;

    public String getNamesapce() {
        return namesapce;
    }
    public void setNamesapce(String namesapce) {
        this.namesapce = namesapce;
    }
    public String getIndexName() {
        return indexName;
    }
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }
    public String getSet() {
        return set;
    }
    public void setSet(String set) {
        this.set = set;
    }
    public String getBin() {
        return bin;
    }
    public void setBin(String bin) {
        this.bin = bin;
    }
    public IndexType getIndexType() {
        return indexType;
    }
    public void setIndexType(IndexType indexType) {
        this.indexType = indexType;
    }
    public IndexCollectionType getIndexCollectionType() {
        return indexCollectionType;
    }
    public void setIndexCollectionType(IndexCollectionType indexCollectionType) {
        this.indexCollectionType = indexCollectionType;
    }
    public IndexState getState() {
        return state;
    }
    public void setState(IndexState state) {
        this.state = state;
    }

    @Override
    public int hashCode() {
        return 3 * (this.namesapce == null ? 0 : this.namesapce.hashCode()) +
                5 * (this.set == null ? 0 : this.set.hashCode()) +
                7 * (this.bin == null ? 0 : this.bin.hashCode()) +
                11 * (this.indexName == null ? 0 : this.indexName.hashCode()) +
                13 * (this.indexCollectionType == null ? 0 : this.indexCollectionType.hashCode()) +
                17 * (this.indexType == null ? 0 : this.indexType.hashCode()) +
                19 * (this.state == null ? 0 : this.state.hashCode());
    }
    
    private boolean equals(Object s1, Object s2) {
        return (s1 == null && s2 == null) || (s1 != null && s1.equals(s2));
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        else if (!(obj instanceof Sindex)) {
            return false;
        }
        Sindex other = (Sindex)obj;
        return equals(this.namesapce, other.namesapce) && 
                equals(this.set, other.set) && 
                equals(this.bin, other.bin) && 
                equals(this.indexName, other.indexName) &&
                equals(this.indexType, other.indexType) && 
                equals(this.indexCollectionType, other.indexCollectionType) &&
                equals(this.state, other.state);
    }
    
    @Override
    public boolean isEqualish(Object obj) {
        if (this == obj) {
            return true;
        }
        else if (!(obj instanceof Sindex)) {
            return false;
        }
        Sindex other = (Sindex)obj;
        return equals(this.namesapce, other.namesapce) && 
                equals(this.set, other.set) && 
                equals(this.bin, other.bin) && 
                equals(this.indexName, other.indexName) &&
                equals(this.indexType, other.indexType) && 
                equals(this.indexCollectionType, other.indexCollectionType);
    }
}
