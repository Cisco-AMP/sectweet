package com.cisco.amp.flink.model;

public class TokenCount {
    private String token;
    private int count;

    @Override
    public String toString() {
        return String.format("%s, %d", getToken(), getCount());
    }

    public String getToken() {
        return token;
    }

    public TokenCount(String token, int count) {
        this.token = token;
        this.count = count;
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
