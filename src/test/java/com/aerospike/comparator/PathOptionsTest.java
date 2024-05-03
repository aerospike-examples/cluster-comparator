package com.aerospike.comparator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.aerospike.client.Key;
import com.aerospike.client.Record;

public class PathOptionsTest {
    @Test
    public void fullPathMatch() {
        PathOption option = new PathOption("/test/compSet/ignoreDiff", PathAction.IGNORE);
        Deque<String> parts = new ArrayDeque<>();
        parts.add("ignoreDiff");
        parts.add("compSet");
        parts.add("test");
        assertTrue(option.matches(parts));
        
        parts = new ArrayDeque<>();
        parts.add("ignoreSame");
        parts.add("compSet");
        parts.add("test");
        assertFalse(option.matches(parts));
    }
    
    @Test
    public void pathWithSingleWildcardAtEndMatch() {
        PathOption option = new PathOption("/test/compSet/*", PathAction.IGNORE);
        Deque<String> parts = new ArrayDeque<>();
        parts.add("ignoreDiff");
        parts.add("compSet");
        parts.add("test");
        assertTrue(option.matches(parts));

        parts = new ArrayDeque<>();
        parts.add("ignoreSame");
        parts.add("compSet");
        parts.add("test");
        assertTrue(option.matches(parts));

        parts = new ArrayDeque<>();
        parts.add("ignoreSame");
        parts.add("compSetOther");
        parts.add("test");
        assertFalse(option.matches(parts));
    }
    
    @Test
    public void pathWithSingleWildcardAtStartMatch() {
        PathOption option = new PathOption("/*/compSet/ignoreDiff", PathAction.IGNORE);
        Deque<String> parts = new ArrayDeque<>();
        parts.add("ignoreDiff");
        parts.add("compSet");
        parts.add("test");
        assertTrue(option.matches(parts));

        parts = new ArrayDeque<>();
        parts.add("ignoreSame");
        parts.add("compSet");
        parts.add("testOther");
        assertFalse(option.matches(parts));

        parts = new ArrayDeque<>();
        parts.add("ignoreDiff");
        parts.add("compSet");
        assertFalse(option.matches(parts));
    }
    
    @Test
    public void pathWithSingleWildcardInMiddleMatch() {
        PathOption option = new PathOption("/test/*/ignoreDiff", PathAction.IGNORE);
        Deque<String> parts = new ArrayDeque<>();
        parts.add("ignoreDiff");
        parts.add("compSet");
        parts.add("test");
        assertTrue(option.matches(parts));

        parts = new ArrayDeque<>();
        parts.add("ignoreSame");
        parts.add("compSet");
        parts.add("test");
        assertFalse(option.matches(parts));
    }
    
    @Test
    public void pathWithSingleTrailingWildcard() {
        PathOption option = new PathOption("/test/compSet/ignoreDiff/*", PathAction.IGNORE);
        Deque<String> parts = new ArrayDeque<>();
        parts.add("ignoreDiff");
        parts.add("compSet");
        parts.add("test");
        assertFalse(option.matches(parts));
    }
    
    @Test
    public void pathWithDoubleWildcardAtEndMatch() {
        PathOption option = new PathOption("/test/compSet/**", PathAction.IGNORE);
        Deque<String> parts = new ArrayDeque<>();
        parts.add("ignoreDiff");
        parts.add("compSet");
        parts.add("test");
        assertTrue(option.matches(parts));

        parts = new ArrayDeque<>();
        parts.add("ignoreSame");
        parts.add("compSet");
        parts.add("test");
        assertTrue(option.matches(parts));

        parts = new ArrayDeque<>();
        parts.add("ignoreSame");
        parts.add("compSet");
        parts.add("test");
        assertTrue(option.matches(parts));

        parts = new ArrayDeque<>();
        parts.add("ignoreSame");
        parts.add("ignoreSame");
        parts.add("ignoreSame");
        parts.add("ignoreSame");
        parts.add("compSetOther");
        parts.add("test");
        assertFalse(option.matches(parts));
    }
    
    @Test
    public void pathWithDoubleWildcardAtStartMatch() {
        PathOption option = new PathOption("/**/compSet/ignoreDiff", PathAction.IGNORE);
        Deque<String> parts = new ArrayDeque<>();
        parts.add("ignoreDiff");
        parts.add("compSet");
        parts.add("test");
        assertTrue(option.matches(parts));

        parts = new ArrayDeque<>();
        parts.add("ignoreSame");
        parts.add("compSet");
        parts.add("testOther");
        assertFalse(option.matches(parts));

        parts = new ArrayDeque<>();
        parts.add("ignoreDiff");
        parts.add("compSet");
        assertTrue(option.matches(parts));

        parts = new ArrayDeque<>();
        parts.add("ignoreDiff");
        parts.add("compSet");
        parts.add("compSet1");
        parts.add("compSet2");
        parts.add("compSet3");
        assertTrue(option.matches(parts));
}
    
    @Test
    public void pathWithDoubleWildcardInMiddleMatch() {
        PathOption option = new PathOption("/test/**/ignoreDiff", PathAction.IGNORE);
        Deque<String> parts = new ArrayDeque<>();
        parts.add("ignoreDiff");
        parts.add("compSet");
        parts.add("test");
        assertTrue(option.matches(parts));

        parts = new ArrayDeque<>();
        parts.add("ignoreSame");
        parts.add("compSet");
        parts.add("test");
        assertFalse(option.matches(parts));

        parts = new ArrayDeque<>();
        parts.add("ignoreDiff");
        parts.add("compSet3");
        parts.add("compSet2");
        parts.add("compSet");
        parts.add("test");
        assertTrue(option.matches(parts));

        parts = new ArrayDeque<>();
        parts.add("ignoreOther");
        parts.add("compSet3");
        parts.add("compSet2");
        parts.add("compSet");
        parts.add("test");
        assertFalse(option.matches(parts));
    }

    @Test
    public void pathWithDoubleTrailingWildcard() {
        PathOption option = new PathOption("/test/compSet/ignoreDiff/**", PathAction.IGNORE);
        Deque<String> parts = new ArrayDeque<>();
        parts.add("ignoreDiff");
        parts.add("compSet");
        parts.add("test");
        assertTrue(option.matches(parts));
    }
    
    @Test
    public void testRecordCompare() {
        PathOption option = new PathOption("/test/compSet/ignoreDiff", PathAction.IGNORE);
        PathOptions options = new PathOptions(option);
        
        RecordComparator comparator =new RecordComparator();
        Map<String, Object> map1 = new HashMap<>();
        map1.put("name", "Tim");
        map1.put("age", 312);
        map1.put("ignoreDiff", true);
        
        Map<String, Object> map2 = new HashMap<>();
        map2.put("name", "Tim");
        map2.put("age", 312);
        map2.put("ignoreDiff", false);
        
        Record r1 = new Record(map1, 1, 1);
        Record r2 = new Record(map2, 1, 1);
        Key key = new Key("test", "compSet",1);
        DifferenceSet result = comparator.compare(key, r1, r2, options, false, 0, 1);
        assertFalse(result.areDifferent());

        options = new PathOptions();
        result = comparator.compare(key, r1, r2, options, false, 0, 1);
        assertTrue(result.areDifferent());
    }

    @Test
    public void testRecordCompareWithWildcard() {
        PathOption option = new PathOption("/test/compSet/*", PathAction.IGNORE);
        PathOptions options = new PathOptions(option);
        
        RecordComparator comparator =new RecordComparator();
        Map<String, Object> map1 = new HashMap<>();
        map1.put("name", "Bob");
        map1.put("age", 312);
        map1.put("ignoreDiff", true);
        
        Map<String, Object> map2 = new HashMap<>();
        map2.put("name", "Tim");
        map2.put("age", 312);
        map2.put("ignoreDiff", false);
        
        Record r1 = new Record(map1, 1, 1);
        Record r2 = new Record(map2, 1, 1);
        DifferenceSet result = comparator.compare(new Key("test", "compSet",1), r1, r2, options, false, 0, 1);
        assertFalse(result.areDifferent());

        options = new PathOptions();
        result = comparator.compare(new Key("test", "compSet",1), r1, r2, options, false, 0, 1);
        assertTrue(result.areDifferent());
    }

    @Test
    public void testRecordCompareWithChildMap() {
        PathOption option = new PathOption("/test/compSet/map/789/Aaa", PathAction.IGNORE);
        PathOptions options = new PathOptions(option);
        RecordComparator comparator =new RecordComparator();
        
        Map<String, Object> map1 = new HashMap<>();
        map1.put("name", "Tim");
        map1.put("age", 312);
        map1.put("ignoreDiff", true);
        Map<Object, Object> childMap1 = new HashMap<>();
        childMap1.put(1, "!");
        childMap1.put("A", "b");
        childMap1.put("valid", true);
        Map<Object, Object> childChildMap1 = new HashMap<>();
        childChildMap1.put(123, 456);
        childChildMap1.put("Aaa", "bbb");
        childMap1.put(789, childChildMap1);
        map1.put("map", childMap1);
        
        Map<String, Object> map2 = new HashMap<>();
        map2.put("name", "Tim");
        map2.put("age", 312);
        map2.put("ignoreDiff", true);
        Map<Object, Object> childMap2 = new HashMap<>();
        childMap2.put(1, "!");
        childMap2.put("A", "b");
        childMap2.put("valid", true);
        Map<Object, Object> childChildMap2 = new HashMap<>();
        childChildMap2.put(123, 456);
        childChildMap2.put("Aaa", "aaa");
        childMap2.put(789, childChildMap2);
        map2.put("map", childMap2);
        
        Record r1 = new Record(map1, 1, 1);
        Record r2 = new Record(map2, 1, 1);
        Key key = new Key("test", "compSet",1);
        DifferenceSet result = comparator.compare(key, r1, r2, options, false, 0, 1);
        assertFalse(result.areDifferent());

        options = new PathOptions();
        result = comparator.compare(key, r1, r2, options, false, 0, 1);
        assertTrue(result.areDifferent());
    }

    @Test
    public void testRecordCompareWithChildMapAndWildcard() {
        PathOption option = new PathOption("/test/**/Aaa", PathAction.IGNORE);
        PathOptions options = new PathOptions(option);
        RecordComparator comparator =new RecordComparator();
        
        Map<String, Object> map1 = new HashMap<>();
        map1.put("name", "Tim");
        map1.put("age", 312);
        map1.put("ignoreDiff", true);
        Map<Object, Object> childMap1 = new HashMap<>();
        childMap1.put(1, "!");
        childMap1.put("A", "b");
        childMap1.put("valid", true);
        Map<Object, Object> childChildMap1 = new HashMap<>();
        childChildMap1.put(123, 456);
        childChildMap1.put("Aaa", "bbb");
        childMap1.put(789, childChildMap1);
        map1.put("map", childMap1);
        
        Map<String, Object> map2 = new HashMap<>();
        map2.put("name", "Tim");
        map2.put("age", 312);
        map2.put("ignoreDiff", true);
        Map<Object, Object> childMap2 = new HashMap<>();
        childMap2.put(1, "!");
        childMap2.put("A", "b");
        childMap2.put("valid", true);
        Map<Object, Object> childChildMap2 = new HashMap<>();
        childChildMap2.put(123, 456);
        childChildMap2.put("Aaa", "aaa");
        childMap2.put(789, childChildMap2);
        map2.put("map", childMap2);
        
        Record r1 = new Record(map1, 1, 1);
        Record r2 = new Record(map2, 1, 1);
        Key key = new Key("test", "compSet",1);
        DifferenceSet result = comparator.compare(key, r1, r2, options, false, 0, 1);
        assertFalse(result.areDifferent());

        options = new PathOptions();
        result = comparator.compare(key, r1, r2, options, false, 0, 1);
        assertTrue(result.areDifferent());
    }

    @Test
    public void testRecordCompareWithChildListAndMap() {
        PathOption option = new PathOption("/test/compSet/list/3/Aaa", PathAction.IGNORE);
        PathOptions options = new PathOptions(option);
        RecordComparator comparator =new RecordComparator();
        
        Map<String, Object> map1 = new HashMap<>();
        map1.put("name", "Tim");
        map1.put("age", 312);
        map1.put("ignoreDiff", true);
        List<Object> childList1 = new ArrayList<>();
        childList1.add("!");
        childList1.add("b");
        childList1.add(true);
        Map<Object, Object> childChildMap1 = new HashMap<>();
        childChildMap1.put(123, 456);
        childChildMap1.put("Aaa", "bbb");
        childList1.add(childChildMap1);
        map1.put("list", childList1);
        
        Map<String, Object> map2 = new HashMap<>();
        map2.put("name", "Tim");
        map2.put("age", 312);
        map2.put("ignoreDiff", true);
        List<Object> childList2 = new ArrayList<>();
        childList2.add("!");
        childList2.add("b");
        childList2.add(true);
        Map<Object, Object> childChildMap2 = new HashMap<>();
        childChildMap2.put(123, 456);
        childChildMap2.put("Aaa", "aaa");
        childList2.add(childChildMap2);
        map2.put("list", childList2);
        
        Record r1 = new Record(map1, 1, 1);
        Record r2 = new Record(map2, 1, 1);
        Key key = new Key("test", "compSet",1);
        DifferenceSet result = comparator.compare(key, r1, r2, options, false, 0, 1);
        assertFalse(result.areDifferent());

        options = new PathOptions();
        result = comparator.compare(key, r1, r2, options, false, 0, 1);
        assertTrue(result.areDifferent());
    }

    @Test
    public void testRecordCompareWithChildList() {
        PathOption option = new PathOption("/test/compSet/list/2", PathAction.IGNORE);
        PathOptions options = new PathOptions(option);
        RecordComparator comparator =new RecordComparator();
        
        Map<String, Object> map1 = new HashMap<>();
        map1.put("name", "Tim");
        map1.put("age", 312);
        map1.put("ignoreDiff", true);
        List<Object> childList1 = new ArrayList<>();
        childList1.add("!");
        childList1.add("b");
        childList1.add(true);
        childList1.add(1);
        map1.put("list", childList1);
        
        Map<String, Object> map2 = new HashMap<>();
        map2.put("name", "Tim");
        map2.put("age", 312);
        map2.put("ignoreDiff", true);
        List<Object> childList2 = new ArrayList<>();
        childList2.add("!");
        childList2.add("b");
        childList2.add(false);
        childList2.add(1);
        map2.put("list", childList2);
        
        Record r1 = new Record(map1, 1, 1);
        Record r2 = new Record(map2, 1, 1);
        Key key = new Key("test", "compSet",1);
        DifferenceSet result = comparator.compare(key, r1, r2, options, false, 0, 1);
        assertFalse(result.areDifferent());

        options = new PathOptions();
        result = comparator.compare(key, r1, r2, options, false, 0, 1);
        assertTrue(result.areDifferent());
    }
}
