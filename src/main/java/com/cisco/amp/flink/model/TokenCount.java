package com.cisco.amp.flink.model;

import java.util.Date;

public class TokenCount {
    private String token;
    private int count;
    private long timestamp;

    // Required for Flink to use getters as keys
    public TokenCount() {}

    public TokenCount(String token, int count, long timestamp) {
        this.token = token;
        this.count = count;
        this.timestamp = timestamp;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public long getTimestamp() { return timestamp; }

    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return String.format("%s| %s: %d", new Date(getTimestamp()), getToken(), getCount());
    }

}
