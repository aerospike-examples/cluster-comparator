package com.aerospike.comparator;

import java.util.Map;

interface NameFinder {
    String getName(Map<String, String> object);
}