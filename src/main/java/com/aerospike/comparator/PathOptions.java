package com.aerospike.comparator;

import java.util.Arrays;
import java.util.Deque;
import java.util.EnumSet;
import java.util.List;

public class PathOptions {
    private List<PathOption> paths;
    public PathOptions() {}
    
    public PathOptions(PathOption ...options) {
        this.paths = Arrays.asList(options);
    }

    @Override
    public String toString() {
        return paths == null ? "[]" : paths.toString();
    }
    
    public List<PathOption> getPaths() {
        return paths;
    }

    public void setPaths(List<PathOption> paths) {
        this.paths = paths;
    }
    
    public EnumSet<PathAction> getActionsForPath(Deque<String> path) {
        EnumSet<PathAction> actions = EnumSet.noneOf(PathAction.class);
        if (paths != null) {
            for (PathOption thisPath : paths) {
                if (thisPath.matches(path)) {
                    actions.add(thisPath.getAction());
                }
            }
        }
        return actions;
    }
}
