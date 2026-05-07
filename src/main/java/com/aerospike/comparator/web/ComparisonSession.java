package com.aerospike.comparator.web;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.aerospike.comparator.ClusterComparator;
import com.aerospike.comparator.ClusterComparatorOptions;
import com.aerospike.comparator.DifferenceSummary;

public class ComparisonSession {
    public enum State { IDLE, RUNNING, COMPLETE, ERROR }

    private volatile State state = State.IDLE;
    private volatile ClusterComparator comparator;
    private volatile Thread runThread;
    private volatile String errorMessage;
    private volatile DifferenceSummary lastResult;
    private final List<ProgressSnapshot> completedRuns = Collections.synchronizedList(new ArrayList<>());

    public synchronized String start(String[] args) {
        if (state == State.RUNNING) {
            return "A comparison is already running.";
        }
        try {
            ClusterComparatorOptions opts = new ClusterComparatorOptions(args);
            comparator = new ClusterComparator(opts);
        } catch (Exception e) {
            return "Failed to initialize: " + e.getMessage();
        }
        state = State.RUNNING;
        errorMessage = null;
        lastResult = null;

        runThread = new Thread(() -> {
            try {
                lastResult = comparator.begin();
                captureCompletedRun("COMPLETE");
                state = State.COMPLETE;
            } catch (Exception e) {
                errorMessage = e.getMessage();
                captureCompletedRun("ERROR");
                state = State.ERROR;
            }
        }, "ComparisonRunner");
        runThread.setDaemon(true);
        runThread.start();
        return null;
    }

    private void captureCompletedRun(String finalState) {
        if (comparator == null) return;
        ProgressSnapshot snapshot = comparator.getProgressSnapshot();
        snapshot.setState(finalState);
        snapshot.setCompletedAt(System.currentTimeMillis());
        completedRuns.add(snapshot);
    }

    public void stop() {
        if (comparator != null && state == State.RUNNING) {
            comparator.requestTermination();
        }
    }

    public ProgressSnapshot getProgress() {
        if (comparator == null) {
            return null;
        }
        ProgressSnapshot snapshot = comparator.getProgressSnapshot();
        snapshot.setState(state.name());
        return snapshot;
    }

    public State getState() {
        return state;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public DifferenceSummary getLastResult() {
        return lastResult;
    }

    public List<ProgressSnapshot> getCompletedRuns() {
        return new ArrayList<>(completedRuns);
    }

    public void removeCompletedRun(int index) {
        if (index >= 0 && index < completedRuns.size()) {
            completedRuns.remove(index);
        }
    }

    public void reset() {
        if (state != State.RUNNING) {
            state = State.IDLE;
            comparator = null;
            runThread = null;
            errorMessage = null;
            lastResult = null;
        }
    }
}
