package com.aerospike.comparator;

import com.aerospike.client.Key;
import com.aerospike.client.Value;

public class FileLine {
    private final String namespace;
    private final String partitionId;
    private final String setName;
    private final String userKey;
    private final String[] digests;
    
    public FileLine(String line) {
        String[] linePart = line.split(",");
        int numberOfClusters = 2;
        int digestOffset = 4;
        this.namespace = linePart[0];
        this.setName = linePart[1];
        this.partitionId = linePart[2];
        this.userKey = linePart[3];
        if (linePart[4].matches("\\d+")) {
            numberOfClusters = Integer.parseInt(linePart[4]);
            digestOffset = 5;
        }
        this.digests = new String[numberOfClusters];
        for (int i = 0; i < numberOfClusters; i++) {
            this.digests[i] = parseDigest(linePart, digestOffset + i);
        }
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

    public String getDigest(int digestNumber) {
        return digests[digestNumber];
    }

    
    public boolean hasDigest(int digestNumber) {
        return digestNumber >= 0 && digestNumber < digests.length && !digests[digestNumber].isEmpty();
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
        if (!this.userKey.isEmpty()) {
            return new Key(this.namespace, this.setName, this.userKey);
        }
        for (int i = 0; i < digests.length; i++) {
            if (hasDigest(i)) {
                return new Key(this.namespace, hexStringToByteArray(getDigest(i)), this.setName, Value.get(this.userKey));
            }
        }
        throw new IllegalStateException("Could not find any digest or user key");
    }

    public Key getKey(int i) {
        if (i < 0 || i >= digests.length) {
            throw new IllegalArgumentException(String.format("key must be in the range 0 to %d, not %d",  digests.length, i));
        }
        // Use a digest in preference to a user key as it's difficult to tell the difference between "2" and 2
        if (hasDigest(i)) {
            return new Key(this.namespace, hexStringToByteArray(getDigest(i)), this.setName, Value.get(this.userKey));
        }
        if (userKey != null) {
            return new Key(this.namespace, this.setName, this.userKey);
        }
        throw new IllegalStateException("Could not find any digest or user key");
    }
}
