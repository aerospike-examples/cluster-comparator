package com.aerospike.comparator;

import com.aerospike.comparator.ClusterComparatorOptions.ParseException;

/**
 * Simple class holding a String and an offset. This is useful in parsing data in a string as the current
 * symbol (character) is stored in the offset.
 * @author tfaulkes
 *
 */
class StringWithOffset {
    private int offset = 0;
    private final String data;
    public StringWithOffset(String data) {
        this.data = data;
    }
    public boolean checkAndConsumeSymbol(char ch) {
        return checkAndConsumeSymbol(ch, true);
    }
    private boolean skipWhitespace() {
        while (offset < data.length() && Character.isWhitespace(data.charAt(offset))) {
            offset++;
        }
        return offset < data.length();
    }
    
    public boolean checkAndConsumeSymbol(char ch, boolean mandatory) {
        if (this.skipWhitespace()) {
            if (data.charAt(offset) == ch) {
                offset++;
                return true;
            }
            else {
                throw new ParseException("Expected '" + ch + "' but received '" + data.charAt(offset) + "'");
            }
        }
        if (mandatory) {
            throw new ParseException("Expected '" + ch + "' but received end of input");
        }
        else {
            return false;
        }
    }
    
    public boolean isSymbol(char ch) {
        return isSymbol(ch, true);
    }
    
    public boolean isSymbol(char ch, boolean consumeSymbol) {
        if (skipWhitespace() && data.charAt(offset) == ch) {
            if (consumeSymbol) {
                offset++;
            }
            return true;
        }
        return false;
    }

    public String getString() {
        if (skipWhitespace()) {
            if (data.charAt(offset)=='"') {
                offset++;
                int next = data.indexOf('"', offset);
                if (next < 0) {
                    throw new ParseException("Expected a string no matching end of string found");
                }
                else {
                    String result = data.substring(offset, next);
                    offset = next+1;
                    return result;
                }
            }
            else if (data.charAt(offset)=='{') {
                int start = offset;
                // Nested object, scan to the end of the nesting
                int endBraceCount = 1;
                offset++;
                while (endBraceCount > 0 && offset < data.length()) {
                    if (data.charAt(offset) == '{') {
                        endBraceCount++;
                    }
                    else if (data.charAt(offset) == '}') {
                        endBraceCount--;
                    }
                    offset++;
                }
                if (endBraceCount == 0) {
                    return data.substring(start, offset);
                }
                else {
                    throw new ParseException(String.format("Error whilst parsing string: '%s': received '{' to start object but didn't having a matching closing '}'"));
                }
            }
            else {
                int start = offset;
                while (offset < data.length() && Character.isJavaIdentifierPart(data.charAt(offset))) {
                    offset++;
                }
                return data.substring(start, offset);
            }
        }
        return null;
    }
    
    @Override
    public String toString() {
        return String.format("(offset:%d,current:%s)[%s]", offset, offset < data.length()? data.substring(offset, offset+1) : "EOS" , data);
    }
}