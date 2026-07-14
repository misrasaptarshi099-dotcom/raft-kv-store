package com.raft.node;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LogEntry {
    private final long index;
    private final long term;
    private final String command;

    @JsonCreator
    public LogEntry(
            @JsonProperty("index") long index,
            @JsonProperty("term") long term,
            @JsonProperty("command") String command) {
        this.index = index;
        this.term = term;
        this.command = command;
    }

    public long getIndex() {
        return index;
    }

    public long getTerm() {
        return term;
    }

    public String getCommand() {
        return command;
    }

    @Override
    public String toString() {
        return "LogEntry{index=" + index + ", term=" + term + ", command='" + command + "'}";
    }
}
