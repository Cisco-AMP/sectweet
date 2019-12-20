package com.cisco.amp.flink.model;

public class TokenCount {
    private String token;
    private int count;

    // Required for Flink to use getters as keys
    public TokenCount() {}

    public TokenCount(String token, int count) {
        this.token = token;
        this.count = count;
    }

    @Override
    public String toString() {
        return String.format("%s, %d", getToken(), getCount());
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
}
