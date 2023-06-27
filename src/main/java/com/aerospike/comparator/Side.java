package com.aerospike.comparator;

public enum Side {
    SIDE_1(1),
    SIDE_2(2);
    
    public final int value;
    private Side(int value) {
        this.value = value;
    }
}