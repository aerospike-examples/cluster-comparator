package com.aerospike.comparator.metadata;

public class NamedData<T> {
    private final String name;
    private final T data;
    public NamedData(String name, T data) {
        this.name = name;
        this.data = data;
    }
    
    public String getName() {
        return name;
    }
    public T getData() {
        return data;
    }
    @Override
    public String toString() {
        return this.name + "=" + this.data;
    }
}