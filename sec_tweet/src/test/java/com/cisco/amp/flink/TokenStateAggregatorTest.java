package com.cisco.amp.flink;

import com.cisco.amp.flink.model.TokenCount;
import com.cisco.amp.flink.model.TokenTrend;

import static org.junit.Assert.*;

public class TokenStateAggregatorTest {
    @org.junit.Test
    public void add() {
        TokenTrend tokenTrend = new TokenTrend();
        tokenTrend.setToken("token");
        tokenTrend.setEntries(2);
        tokenTrend.setCount(6);
        tokenTrend.setLastCount(3);

        TokenCount tokenCount = new TokenCount("token", 5);
        TokenStateAggregator agg = new TokenStateAggregator(0.0001f);
        TokenTrend result = agg.add(tokenCount, tokenTrend);

        assertEquals("token", result.getToken());
        assertEquals(3, result.getEntries());
        assertEquals(5, result.getLastCount());
        assertEquals(11, result.getCount());
    }

    @org.junit.Test
    public void getResult_exactlyEqual() {
        TokenTrend tokenTrend = new TokenTrend();
        tokenTrend.setToken("token");
        tokenTrend.setEntries(2);
        tokenTrend.setCount(6);
        tokenTrend.setLastCount(3);
        TokenStateAggregator agg = new TokenStateAggregator(0.0001f);
        assertEquals(TokenTrend.State.NO_CHANGE, agg.getResult(tokenTrend));
    }

    @org.junit.Test
    public void getResult_approximatelyEqual() {
        TokenTrend tokenTrend = new TokenTrend();
        tokenTrend.setToken("token");
        tokenTrend.setEntries(2);
        tokenTrend.setCount(7);
        tokenTrend.setLastCount(3);
        TokenStateAggregator agg = new TokenStateAggregator(0.143f);
        assertEquals(TokenTrend.State.NO_CHANGE, agg.getResult(tokenTrend));
    }

    @org.junit.Test
    public void getResult_decreasing() {
        TokenTrend tokenTrend = new TokenTrend();
        tokenTrend.setToken("token");
        tokenTrend.setEntries(2);
        tokenTrend.setCount(6);
        tokenTrend.setLastCount(2);
        TokenStateAggregator agg = new TokenStateAggregator(0.0001f);
        assertEquals(TokenTrend.State.DECREASING, agg.getResult(tokenTrend));
    }

    @org.junit.Test
    public void getResult_increasing() {
        TokenTrend tokenTrend = new TokenTrend();
        tokenTrend.setToken("token");
        tokenTrend.setEntries(2);
        tokenTrend.setCount(6);
        tokenTrend.setLastCount(4);
        TokenStateAggregator agg = new TokenStateAggregator(0.0001f);
        assertEquals(TokenTrend.State.INCREASING, agg.getResult(tokenTrend));
    }

    @org.junit.Test
    public void merge() {
        TokenTrend tokenTrendA = new TokenTrend();
        tokenTrendA.setToken("tokenA");
        tokenTrendA.setEntries(2);
        tokenTrendA.setCount(4);
        tokenTrendA.setLastCount(3);

        TokenTrend tokenTrendB = new TokenTrend();
        tokenTrendB.setToken("tokenB");
        tokenTrendB.setEntries(2);
        tokenTrendB.setCount(6);
        tokenTrendB.setLastCount(1);

        TokenStateAggregator agg = new TokenStateAggregator(0.0001f);
        TokenTrend result = agg.merge(tokenTrendA, tokenTrendB);
        assertEquals(result.getToken(), "tokenA");
        assertEquals(result.getEntries(), 4);
        assertEquals(result.getCount(), 10);
        assertEquals(result.getLastCount(), 3);
    }
}