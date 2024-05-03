package com.aerospike.comparator;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

public class PathOption {
    private String path;
    private PathAction action;
    private String[] pathParts;
    
    public PathOption() {}
    public PathOption(String path, PathAction action) {
        this.setPath(path);
        this.action = action;
    }

    @Override
    public String toString() {
        return getPath() + " " + action;
    }
    public String getPath() {
        return path;
    }
    public void setPath(String path) {
        this.path = path;
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        this.pathParts = path.split("/");
        for (int i = 0; i < this.pathParts.length; i++) {
            String thisPart = this.pathParts[i];
            if ("**".equals(thisPart) || "*".equals(thisPart)) {
                continue;
            }
            else if (thisPart.indexOf("*") >= 0) {
                throw new IllegalArgumentException("Path cannot contain wildcards ('*', '**'), except as entire path parts. Path specified was " + path);
            }
        }
    }
    
    public PathAction getAction() {
        return action;
    }
    public void setAction(PathAction action) {
        this.action = action;
    }
    
    public boolean matches(Deque<String> pathParts) {
        int index = 0;
        boolean isOnWildcard = false;
        for (Iterator<String> iter = pathParts.descendingIterator(); iter.hasNext();) {
            String thispath = iter.next();
            if (index >= this.pathParts.length) {
                return false;
            }
            if ("*".equals(this.pathParts[index])) {
                // This has to match
                index++;
                continue;
            }
            else if ("**".equals(this.pathParts[index])) {
                isOnWildcard = true;
                index++;
                if (index >= this.pathParts.length) {
                    return true;
                }
            }
            if (isOnWildcard) {
                if (index >= this.pathParts.length) {
                    // This has a wildcard at the end, it matches
                    return true;
                }
                else {
                    // Does this part of the path match the next fixed part?
                    if (thispath.equals(this.pathParts[index])) {
                        isOnWildcard = false;
                        index++;
                    }
                }
            }
            else {
                if (thispath.equals(this.pathParts[index])) {
                    index++;
                }
                else {
                    return false;
                }
            }
        }
        if (index < this.pathParts.length) {
            // This matches only if the remaining items are ** wildcards
            for (int i = index; i < this.pathParts.length; i++) {
                if (!"**".equals(this.pathParts[index])) {
                    return false;
                }
            }
            return true;
        }
//        if (!isOnWildcard && index < this.pathParts.length) {
//            return false;
//        }
        return true;
    }
    
    public static void main(String[] args) {
        PathOption option = new PathOption("/*/part1/**/name", PathAction.IGNORE);
        Deque<String> parts = new ArrayDeque<>();
        parts.add("bin");
        parts.add("part1");
        parts.add("part2");
        parts.add("12");
        parts.add("name");
        System.out.println(option.matches(parts));
        
        option = new PathOption("/test/compSet/ignoreDiff", PathAction.IGNORE);
        parts = new ArrayDeque<>();
        parts.add("compSet");
        parts.add("test");
        System.out.println(option.matches(parts) + " - should be false");
        
        option = new PathOption("/test/compSet/*", PathAction.IGNORE);
        parts = new ArrayDeque<>();
        parts.add("compSet");
        parts.add("test");
        System.out.println(option.matches(parts) + " - should be false");
        
        option = new PathOption("/test/compSet/**", PathAction.IGNORE);
        parts = new ArrayDeque<>();
        parts.add("compSet");
        parts.add("test");
        System.out.println(option.matches(parts) + " - should be true");
        
        option = new PathOption("/test/compSet", PathAction.IGNORE);
        parts = new ArrayDeque<>();
        parts.add("compSet");
        parts.add("test");
        System.out.println(option.matches(parts) + " - should be true");
        
        option = new PathOption("/test/compSet/ignoreDiff", PathAction.IGNORE);
        parts = new ArrayDeque<>();
        parts.add("compSet");
        parts.add("test");
        System.out.println(option.matches(parts) + " - should be false");
        
        option = new PathOption("/test/**/ignoreDiff", PathAction.IGNORE);
        parts = new ArrayDeque<>();
        parts.add("ignoreDiff");
        parts.add("compSet");
        parts.add("test");
        System.out.println(option.matches(parts) + " - should be true");
        
        option = new PathOption("/**/ignoreDiff", PathAction.IGNORE);
        parts = new ArrayDeque<>();
        parts.add("ignoreDiff");
        parts.add("compSet");
        parts.add("test");
        System.out.println(option.matches(parts) + " - should be true");
        
        option = new PathOption("/test/compSet/ignoreDiff/**", PathAction.IGNORE);
        parts = new ArrayDeque<>();
        parts.add("ignoreDiff");
        parts.add("compSet");
        parts.add("test");
        System.out.println(option.matches(parts) + " - should be true");
        
        option = new PathOption("/**", PathAction.IGNORE);
        parts = new ArrayDeque<>();
        parts.add("ignoreDiff");
        parts.add("compSet");
        parts.add("test");
        System.out.println(option.matches(parts) + " - should be true");
    }
}
