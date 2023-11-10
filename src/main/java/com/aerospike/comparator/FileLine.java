package com.aerospike.comparator;

import com.aerospike.client.Key;

public class FileLine {
    private final String namespace;
    private final String setName;
    private final String userKey;
    private final String digest1;
    private final String digest2;
    
    public FileLine(String line) {
        String[] linePart = line.split(",");
        this.namespace = linePart[0];
        this.setName = linePart[1];
        this.userKey = linePart[2];
        this.digest1 = parseDigest(linePart, 3);
        this.digest2 = parseDigest(linePart, 4);
    }
    private String parseDigest(String[] lineParts, int index) {
        if (index < lineParts.length) {
            String value = lineParts[index];
            if (value != null && (!"null".equals(value))) {
                return value.trim();
            }
        }
        return "";
    }

    public String getNamespace() {
        return namespace;
    }

    public String getSetName() {
        return setName;
    }

    public String getUserKey() {
        return userKey;
    }

    public String getDigest1() {
        return digest1;
    }

    public String getDigest2() {
        return digest2;
    }
    
    public boolean hasDigest1() {
        return !digest1.isEmpty();
    }

    public boolean hasDigest2() {
        return !digest2.isEmpty();
    }
    
    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                                 + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }

    public Key getKey() {
        return getKey(null);
    }
    public Key getKey(Side side) {
        if ((side == null && hasDigest1()) || (side == Side.SIDE_1)) {
            return new Key(this.namespace, hexStringToByteArray(digest1), this.setName, null);
        }
        if ((side == null && hasDigest2()) || (side == Side.SIDE_2)) {
            return new Key(this.namespace, hexStringToByteArray(digest2), this.setName, null);
        }
        return new Key(this.namespace, this.setName, this.userKey);
    }
}
