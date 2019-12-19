package com.cisco.amp.flink.model;

public class TokenTrend {
    private String token;
    private int count = 0;
    private int entries = 0;
    private int lastCount = 0;

    public TokenTrend() {
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

    public int getEntries() {
        return entries;
    }

    public void setEntries(int entries) {
        this.entries = entries;
    }

    public int getLastCount() {
        return lastCount;
    }

    public void setLastCount(int lastCount) {
        this.lastCount = lastCount;
    }

    public enum State
    {
        INCREASING, DECREASING, NO_CHANGE
    }


}
