package com.cisco.amp.flink.model;

public class TokenTrend {
    private String token;
    private State state;

    // Required for Flink to use getters as keys
    public TokenTrend(){}

    public TokenTrend(String token, State state) {
        this.token = token;
        this.state = state;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public enum State
    {
        INCREASING, DECREASING, NO_CHANGE
    }

    @Override
    public String toString() {
        return getToken() + ": " + getState();
    }

}
