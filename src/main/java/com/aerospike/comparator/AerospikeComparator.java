package com.aerospike.comparator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * This class implements a comparator instance which mirrors the comparator Aerospike 
 * uses on the server. This is type based, so
 * <pre>
 *   NULL &lt; Boolean &lt; Integer &lt; String &lt; List &lt Map &lt; Bytes &lt; Double
 * </pre>
 * 
 * Within these types, natural ordering applies.
 * @author tfaulkes
 *
 */
public class AerospikeComparator implements Comparator<Object> {
    private static enum AsType {
        NULL (1),
        BOOLEAN (2),
        INTEGER (3),
        STRING (4),
        LIST (5),
        MAP (6),
        BYTES (7),
        DOUBLE (8),
        OTHER (9);
        
        private int value;
        private AsType(int value) {
            this.value = value;
        }
        
        public int getOrdinal() {
            return value;
        }
    }
    private boolean isByteType(Class<?> clazz) {
        return Byte.class.equals(clazz) ||
                Byte.TYPE.equals(clazz);
    }

    private boolean isIntegerType(Object o) {
        return ((o instanceof Byte) || (o instanceof Character) || (o instanceof Short) || (o instanceof Integer) || (o instanceof Long));
    }
    private boolean isFloatType(Object o) {
        return ((o instanceof Float) || (o instanceof Double));
    }
    
    private AsType getType(Object o) {
        if (o == null) { return AsType.NULL; }
        else if (o instanceof Boolean) { return AsType.BOOLEAN; }
        else if (isIntegerType(o)) { return AsType.INTEGER; }
        else if (o instanceof String) { return AsType.STRING; }
        else if (o instanceof List) { return AsType.LIST; }
        else if (o instanceof Map) { return AsType.MAP; }
        else if (o.getClass().isArray() && isByteType(o.getClass().getComponentType())) { return AsType.BYTES; }
        else if (isFloatType(o)) { return AsType.DOUBLE; }
        else {
            return AsType.OTHER;
        }
    }
    
    private int compareList(List<Object> l1, List<Object> l2) {
        int l1Size = l1.size();
        int l2Size = l2.size();
        for (int index = 0; index < l1.size(); index++) {
            if (index >= l2Size) {
                // l1 is longer
                return -1;
            }
            int result = compare(l1.get(index), l2.get(index));
            if (result != 0) {
                return result;
            }
        }
        return l1Size == l2Size ? 0 : 1;
    }
    
    private int compareMap(Map<Object, Object> m1, Map<Object, Object> m2) {
        // Maps sort on the number of keys, then each key in sorted order
        if (m1.size() == m2.size()) {
            List<Object> sortedKeys1 = new ArrayList<Object>(m1.keySet());
            Collections.sort(sortedKeys1, this);
            List<Object> sortedKeys2 = new ArrayList<Object>(m2.keySet());
            Collections.sort(sortedKeys2, this);
            int result = compareList(sortedKeys1, sortedKeys2);
            if (result != 0) {
                return result;
            }
            // Go value by value in sorted key order
            for (int i = 0; i < sortedKeys1.size(); i++) {
                Object v1 = m1.get(sortedKeys1.get(i));
                Object v2 = m2.get(sortedKeys2.get(i));
                result = this.compare(v1, v2);
                if (result != 0) {
                    return result;
                }
            }
            return 0;
        }
        else {
            return m1.size() - m2.size();
        }
    }
    
    @Override
    public int compare(Object o1, Object o2) {
        return compare(o1, o2, false);
    }
    
    @SuppressWarnings("unchecked")
    public int compare(Object o1, Object o2, boolean replaceMapsWithOrderedLists) {
        AsType t1 = getType(o1);
        AsType t2 = getType(o2);
        if (t1.getOrdinal() != t2.getOrdinal()) {
            return t1.getOrdinal() - t2.getOrdinal();
        }
        
        switch (t1) {
        case NULL:
            return 0;
        case BOOLEAN:
            return Boolean.compare((Boolean)o1, (Boolean)o2);
        case INTEGER:
            return Long.compare(((Number)o1).longValue(), ((Number)o2).longValue());
        case STRING:
            return ((String)o1).compareTo((String)o2);
        case LIST:
            return compareList((List<Object>)o1, (List<Object>)o2);
        case MAP:
            return compareMap((Map<Object, Object>)o1, (Map<Object, Object>)o2);
        case BYTES:
            return Arrays.compare((byte [])o1, (byte [])o2);
        case DOUBLE:
            return Double.compare(((Number)o1).doubleValue(), ((Number)o2).doubleValue());
        case OTHER:
        default:
            // This shouldn't happen
            return 0;
        }
    }
    
    public static void main(String[] args) {
        AerospikeComparator c = new AerospikeComparator();
        System.out.println(c.compare(18, "17"));
    }
}