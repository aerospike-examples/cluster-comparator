package com.aerospike.comparator;

import com.aerospike.comparator.RecordComparator.DifferenceType;

public class DifferenceValue {
    private final DifferenceType type;
    private final Object obj1;
    private final Object obj2;
    private final int index;
    
    public DifferenceValue(DifferenceType type, Object obj1, Object obj2) {
        this(type, obj1, obj2, -1);
    }
    public DifferenceValue(DifferenceType type, Object obj1, Object obj2, int index) {
        this.type = type;
        this.obj1 = obj1;
        this.obj2 = obj2;
        this.index = index;
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
    
    public String asJsonFragment() {
        StringBuffer sb = new StringBuffer();
        sb.append("\"type\":\"").append(this.type).append('"');
        if (obj1 != null) {
            sb.append(",\"side1\":").append(this.showObject(obj1, true));
        }
        if (obj2 != null) {
            sb.append(",\"side2\":").append(this.showObject(obj2, true));
        }
        if (this.index >= 0) {
            sb.append(",\"index\":").append(this.index);
        }
        return sb.toString();
    }
    
    private String showByteArray(byte[] bytes) {
        StringBuffer sb = new StringBuffer().append("[");
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
        StringBuffer sb = new StringBuffer().append("\"");
        for (int i = 0; i < bytes.length; i++) {
            sb.append(String.format("%02X", Byte.toUnsignedInt(bytes[i])));
            if (i < bytes.length-1) {
                sb.append(' ');
            }
        }
        return sb.append("\"").toString();
    }
    
    private String showObject(Object obj) {
        return showObject(obj, false);
    }
    
    private String showObject(Object obj, boolean asJson) {
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
                    return showByteArrayAsJson((byte[])obj);
                }
                else {
                    return showByteArray((byte[])obj);
                }
            }
        }
        return obj.toString();
    }
    @Override
    public String toString() {
        return String.format("{type: %s, side1: %s, side2: %s}", this.getType(), this.showObject(this.getObj1()), this.showObject(this.getObj2()));
    }
}
