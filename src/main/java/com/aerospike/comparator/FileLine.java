package com.aerospike.comparator;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.Key;

public class FileLine {
    private final String namespace;
    private final String partitionId;
    private final String setName;
    private final String userKey;
    private final String[] digests;
    private final NamespaceNameResolver resolver;

    public FileLine(String line, NamespaceNameResolver resolver) {
        this(line, resolver, false);
    }

    public FileLine(String line, NamespaceNameResolver resolver, boolean showMetadata) {
        List<String> linePart = splitCsv(line);
        int numberOfClusters = 2;
        int digestOffset = 4;
        this.namespace = linePart.get(0);
        this.setName = linePart.get(1);
        this.partitionId = linePart.get(2);
        this.userKey = linePart.get(3);
        this.resolver = resolver;
        if (linePart.get(4).matches("\\d+")) {
            numberOfClusters = Integer.parseInt(linePart.get(4));
            digestOffset = 5;
        }
        this.digests = new String[numberOfClusters];
        int stride = showMetadata ? 5 : 1;
        for (int i = 0; i < numberOfClusters; i++) {
            this.digests[i] = parseDigest(linePart, digestOffset + (i * stride));
        }
    }

    /**
     * Split a CSV line. Quoted fields may contain commas, quotes, and newlines.
     * A doubled quote inside a quoted field is one quote character.
     */
    static List<String> splitCsv(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        current.append('"');
                        i++;
                    }
                    else {
                        inQuotes = false;
                    }
                }
                else {
                    current.append(c);
                }
            }
            else if (c == '"') {
                inQuotes = true;
            }
            else if (c == ',') {
                fields.add(current.toString());
                current.setLength(0);
            }
            else {
                current.append(c);
            }
        }
        fields.add(current.toString());
        return fields;
    }

    private String parseDigest(List<String> lineParts, int index) {
        if (index < lineParts.size()) {
            String value = lineParts.get(index);
            if (value != null && (!"null".equals(value)) && !value.isEmpty()) {
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

    public String getDigest() {
        for (int i = 0; i < digests.length; i++) {
            if (digests[i] != null && !digests[i].isEmpty()) {
                return digests[i];
            }
        }
        return null;
    }

    public String getPartitionId() {
        return partitionId;
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
        for (int i = 0; i < digests.length; i++) {
            if (hasDigest(i)) {
                return getKey(i);
            }
        }
        throw new IllegalStateException("Could not find any valid digest");
    }

    public Key getKey(int i) {
        if (i < 0 || i >= digests.length) {
            throw new IllegalArgumentException(String.format("key must be in the range 0 to %d, not %d",  digests.length, i));
        }
        // Use a digest in preference to a user key. The CSV text cannot tell an integer key
        // from the string "2", and a missing user key is written as an empty field.
        String digest = getDigest();
        String set = this.setName == null || this.setName.isEmpty() ? null : this.setName;
        if (digest != null) {
            return new Key(resolver.getNamespaceNameViaSource(this.namespace, i), hexStringToByteArray(digest), set, null);
        }
        if (userKey != null && !userKey.isEmpty() && !"null".equals(userKey)) {
            return new Key(resolver.getNamespaceNameViaSource(this.namespace, i), set, this.userKey);
        }
        throw new IllegalStateException("Could not find any digest or user key");
    }
}
