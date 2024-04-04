package com.aerospike.comparator;

import com.aerospike.comparator.RecordComparator.DifferenceType;

public class DifferenceValue {
    private final DifferenceType type;
    private final Object obj1;
    private final Object obj2;
    private final int cluster1;
    private final int cluster2;
    private final int index;
    
    public DifferenceValue(DifferenceType type, Object obj1, Object obj2, int cluster1, int cluster2) {
        this(type, obj1, obj2, -1, cluster1, cluster2);
    }
    public DifferenceValue(DifferenceType type, Object obj1, Object obj2, int index, int cluster1, int cluster2) {
        this.type = type;
        this.obj1 = obj1;
        this.obj2 = obj2;
        this.index = index;
        this.cluster1 = cluster1;
        this.cluster2 = cluster2;
    }
    public DifferenceType getType() {
        return type;
    }
    public Object getObj1() {
        return obj1;
    }
    public Object getObj2() {
        return obj2;
    }
    
    public String asJsonFragment(boolean truncateBinary) {
        StringBuilder sb = new StringBuilder();
        sb.append("\"type\":\"").append(this.type).append('"');
        if (obj1 != null) {
            sb.append(",\"cluster").append(cluster1+1).append("\":").append(this.showObject(obj1, true, truncateBinary));
        }
        if (obj2 != null) {
            sb.append(",\"cluster").append(cluster2+1).append("\":").append(this.showObject(obj2, true, truncateBinary));
        }
        if (this.index >= 0) {
            sb.append(",\"index\":").append(this.index);
        }
        return sb.toString();
    }
    
    private String showByteArray(byte[] bytes) {
        StringBuilder sb = new StringBuilder().append("[");
        int i;
        int startIndex = index >= 0 ? Math.max(0, index - 5) : 0;
        if (startIndex > 0) {
            sb.append("... ");
        }
        for (i = startIndex; i < bytes.length && i < startIndex + 20; i++) {
            if (i == index) {
                sb.append(String.format("(IDX:%d)<<0x%02x>> ", index, Byte.toUnsignedInt(bytes[i])));
            }
            else {
                sb.append(String.format("0x%02x ", Byte.toUnsignedInt(bytes[i])));
            }
        }
        if (i < bytes.length) {
            sb.append("...");
        }
        return sb.append("]").toString();
    }
    private String showByteArrayAsJson(byte[] bytes) {
        StringBuilder sb = new StringBuilder().append("\"");
        for (int i = 0; i < bytes.length; i++) {
            sb.append(String.format("%02X", Byte.toUnsignedInt(bytes[i])));
            if (i < bytes.length-1) {
                sb.append(' ');
            }
        }
        return sb.append("\"").toString();
    }
    
    private String showObject(Object obj) {
        return showObject(obj, false, false);
    }
    
    private String showObject(Object obj, boolean asJson, boolean truncateBinary) {
        if (obj == null) {
            return "<null>";
        }
        else if (obj instanceof String) {
            return "\"" + ((String)obj) + "\"";
        }
        else if (obj.getClass().isArray()) {
            Class<?> elementType = obj.getClass().getComponentType();
            if (RecordComparator.isByteType(elementType)) {
                if (asJson) {
                    if (truncateBinary) {
                        return "\"" + showByteArray((byte[])obj) +"\"";
                    }
                    else {
                        return showByteArrayAsJson((byte[])obj);
                    }
                }
                else {
                    return showByteArray((byte[])obj);
                }
            }
        }
        return obj.toString();
    }
    
    public int getCluster1() {
        return cluster1;
    }
    public int getCluster2() {
        return cluster2;
    }
    @Override
    public String toString() {
        return String.format("{type: %s, cluster%d: %s, cluster%d: %s}", this.getType(), 
                this.cluster1+1, this.showObject(this.getObj1()), 
                this.cluster2+1, this.showObject(this.getObj2()));
    }
}
