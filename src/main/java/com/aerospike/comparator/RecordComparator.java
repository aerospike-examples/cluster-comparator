package com.aerospike.comparator;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.aerospike.client.Key;
import com.aerospike.client.Record;

public class RecordComparator {
    public enum DifferenceType {
        ONLY_ON_1, ONLY_ON_2, CONTENTS
    }

    protected static boolean isByteType(Class<?> clazz) {
        return Byte.class.equals(clazz) ||
                Byte.TYPE.equals(clazz);
    }

    private void compareNonNull(Object obj1, Object obj2, String path, DifferenceSet differences) {
        if (obj1 instanceof Map) {
            differences.pushPath(path);
            compare((Map<Object, Object>)obj1, (Map<Object, Object>)obj2, differences);
            differences.popPath();
        }
        else if (obj1 instanceof List) {
            differences.pushPath(path);
            compare((List<Object>)obj1, (List<Object>)obj2, differences);
            differences.popPath();
        } else if (obj1.getClass().isArray()) {
            Class<?> elementType = obj1.getClass().getComponentType();
            if (isByteType(elementType)) {
                // Byte arrays are natively supported
                int mismatch = Arrays.mismatch((byte[])obj1, (byte[])obj2);
                if (mismatch >= 0) {
                    differences.addDifference(path, DifferenceType.CONTENTS, obj1, obj2, mismatch);
                }
            } else {
                // TODO: Can we even get other arrays here?
            }
        }
        else if (!obj1.equals(obj2)){
            differences.addDifference(path, DifferenceType.CONTENTS, obj1, obj2);
        }

    }

    private void compare(Object obj1, Object obj2, String path, DifferenceSet differences) {
        if (obj1 == null && obj2 == null) {
            return;
        } else if (obj1 == null) {
            differences.addDifference(path, DifferenceType.ONLY_ON_2, obj1, obj2);
        } else if (obj2 == null) {
            differences.addDifference(path, DifferenceType.ONLY_ON_1, obj1, obj2);
        } else if (!obj1.getClass().equals(obj2.getClass())) {
            differences.addDifference(path, DifferenceType.CONTENTS, obj1, obj2);
        } else {
            compareNonNull(obj1, obj2, path, differences);
        }
    }

    private long getHashCode(Object obj, DifferenceSet differences) {
        long hash = 29L;
        if (obj == null) {
            return 0;
        }
        if (obj instanceof Map) {
            Map<?, ?> map = (Map<?, ?>)obj;
            for (Object key: map.keySet()) {
                differences.pushPath(key.toString());
                long keyHash = getHashCode(key, differences);
                long objHash = getHashCode(map.get(key), differences);
                hash = hash * 23 + 31 * keyHash + 17 * objHash + 1;
                differences.popPath();
            }
        }
        else if (obj instanceof List) {
            boolean unorderedCompare = differences.shouldIgnoreCurrentPath();
            List<?> list = (List)obj;
            for (int i = 0; i < list.size(); i++) {
                differences.pushPath(Integer.toString(i));
                long itemHash = getHashCode(list.get(i), differences);
                if (!unorderedCompare) {
                    itemHash += 13*i;
                }
                hash = hash * 7 + 19 * itemHash + 3;
                differences.popPath();
            }
        }
        else if (obj.getClass().isArray()) {
            Object[] objs = (Object[])obj;
            for (Object thisObj : objs) {
                hash = hash * 11 + 3 * getHashCode(thisObj, differences);
            }
        }
        else {
            hash = hash * 3 + 7 * obj.hashCode();
        }
        return hash;
    }
    private void compare(List<?> list1, List<?> list2, DifferenceSet differences) {
        EnumSet<PathAction> pathOptions = differences.getOptionsForCurrentPath();
        if (pathOptions != null && pathOptions.contains(PathAction.IGNORE)) {
            return;
        }
        if (pathOptions != null && pathOptions.contains(PathAction.COMPAREUNORDERED)) {
            if (list1.size() != list2.size()) {
                differences.addDifference("", DifferenceType.CONTENTS, list1, list2);
            }
            else {
                HashMap<Long, Integer> values = new HashMap<>();
                for (int i = 0; i < list1.size(); i++) {
                    long hash = getHashCode(list1.get(i), differences);
                    Integer count = values.get(hash);
                    if (count == null) {
                        values.put(hash, 1);
                    }
                    else {
                        values.put(hash, count+1);
                    }
                }
                boolean checkSize = true;
                for (int i = 0; i < list2.size(); i++) {
                    long hash = getHashCode(list2.get(i), differences);
                    Integer count = values.get(hash);
                    if (count == null || count == 0) {
                        differences.addDifference("", DifferenceType.CONTENTS, list1, list2);
                        checkSize = false;
                        break;
                    }
                    else if (count <= 1) {
                        values.remove(hash);
                    }
                    else {
                        values.put(hash, count - 1);
                    }
                }
                if (checkSize && !values.isEmpty()) {
                    // There are values missing
                    differences.addDifference("", DifferenceType.CONTENTS, list1, list2);
                }
            }
        }
        else {
            int max = Math.max(list1.size(), list2.size());
            for (int i = 0; i < max; i++) {
                Object o1 = (i < list1.size()) ? list1.get(i) : null;
                Object o2 = (i < list2.size()) ? list2.get(i) : null;
                compare(o1, o2, Integer.toString(i), differences);
                if (differences.isQuickCompare() && differences.areDifferent()) {
                    return;
                }
            }
        }
    }

    private void compare(Map<?, ?> side1, Map<?, ?> side2, DifferenceSet differences) {
        if (differences.shouldIgnoreCurrentPath()) {
            return;
        }

        Set<Object> keys = new HashSet<>();
        for (Object key : side1.keySet()) {
            keys.add(key);
            compare(side1.get(key), side2.get(key), key.toString(), differences);
            if (differences.isQuickCompare() && differences.areDifferent()) {
                return;
            }
        }
        for (Object key : side2.keySet()) {
            if (!keys.contains(key)) {
                compare(side1.get(key), side2.get(key), key.toString(), differences);
                if (differences.isQuickCompare() && differences.areDifferent()) {
                    return;
                }
            }
        }
    }

    public DifferenceSet compare(Key key, Record record1, Record record2, PathOptions pathOptions, boolean stopAtFirstDifference) {
        Map<?, ?> map1 = (Map<?, ?>)record1.bins;
        Map<?, ?> map2 = record2.bins;

        DifferenceSet result = new DifferenceSet(key, stopAtFirstDifference, pathOptions);
        result.pushPath(key.namespace);
        result.pushPath(key.setName);
        compare(map1, map2, result);
        result.popPath();
        result.popPath();
        return result;
    }

    
    public static Map<String, Object> createMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("key1", 1);
        map.put("key2", "tim");
        map.put("key3", new byte[] { (byte) 0xff, (byte) 0xfe, (byte) 0xcd, 0x00, 0x01, 0x7f });
        Map<Object, Object> m2 = new HashMap<>();
        m2.put(1, 1);
        map.put("list", Arrays.asList(17, "bob", 329, "a String"));
        Map<String, Object> m = new HashMap<>();
        m.put("a", "aaa");
        m.put("b", "aaa4");
        m.put("c", "aa3a");
        m.put("d", "a2aa");
        map.put("map", m);
        return map;
    }

    public static void main(String[] args) {
        Map<String, Object> map1 = createMap();
        map1.put("key1", 2);
        map1.put("key5", "only key 5");

        Map<String, Object> map2 = createMap();
        map2.put("key6", "map2, key6");
        ((Map<String, Object>)map2.get("map")).put("e", "Only on 2");
        
        ((byte [])map2.get("key3"))[3] = 12;
        Record r1 = new Record(map1, 0, 0);
        Record r2 = new Record(map2, 0, 0);
        
        PathOptions options = new PathOptions();
        options.setPaths(Arrays.asList(new PathOption("/unordered", PathAction.COMPAREUNORDERED)));
        DifferenceSet diffs = new RecordComparator().compare(null, r1, r2, options, false);
        System.out.println(diffs.toString());
        
    }
}
