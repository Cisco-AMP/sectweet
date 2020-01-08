package com.cisco.amp.flink;

import com.cisco.amp.flink.model.TokenCount;
import com.cisco.amp.flink.model.TokenTrend;
import com.cisco.amp.flink.model.TokenTrendAccumulator;

import static org.junit.Assert.*;

public class TokenStateAggregatorTest {
    @org.junit.Test
    public void add() {
        TokenTrendAccumulator tokenTrendAccumulator = new TokenTrendAccumulator();
        tokenTrendAccumulator.setToken("token");
        tokenTrendAccumulator.setEntries(2);
        tokenTrendAccumulator.setCount(6);
        tokenTrendAccumulator.setLastCount(3);

        TokenCount tokenCount = new TokenCount("token", 5, 10000);
        TokenStateAggregator agg = new TokenStateAggregator(0.0001f);
        TokenTrendAccumulator result = agg.add(tokenCount, tokenTrendAccumulator);

        assertEquals("token", result.getToken());
        assertEquals(3, result.getEntries());
        assertEquals(5, result.getLastCount());
        assertEquals(11, result.getCount());
    }

    @org.junit.Test
    public void getResult_exactlyEqual() {
        TokenTrendAccumulator tokenTrendAccumulator = new TokenTrendAccumulator();
        tokenTrendAccumulator.setToken("token");
        tokenTrendAccumulator.setEntries(2);
        tokenTrendAccumulator.setCount(6);
        tokenTrendAccumulator.setLastCount(3);
        TokenStateAggregator agg = new TokenStateAggregator(0.0001f);
        assertEquals(TokenTrend.State.NO_CHANGE, agg.getResult(tokenTrendAccumulator).getState());
    }

    @org.junit.Test
    public void getResult_approximatelyEqual() {
        TokenTrendAccumulator tokenTrendAccumulator = new TokenTrendAccumulator();
        tokenTrendAccumulator.setToken("token");
        tokenTrendAccumulator.setEntries(2);
        tokenTrendAccumulator.setCount(7);
        tokenTrendAccumulator.setLastCount(3);
        TokenStateAggregator agg = new TokenStateAggregator(0.143f);
        assertEquals(TokenTrend.State.NO_CHANGE, agg.getResult(tokenTrendAccumulator).getState());
    }

    @org.junit.Test
    public void getResult_decreasing() {
        TokenTrendAccumulator tokenTrendAccumulator = new TokenTrendAccumulator();
        tokenTrendAccumulator.setToken("token");
        tokenTrendAccumulator.setEntries(2);
        tokenTrendAccumulator.setCount(6);
        tokenTrendAccumulator.setLastCount(2);
        TokenStateAggregator agg = new TokenStateAggregator(0.0001f);
        assertEquals(TokenTrend.State.DECREASING, agg.getResult(tokenTrendAccumulator).getState());
    }

    @org.junit.Test
    public void getResult_increasing() {
        TokenTrendAccumulator tokenTrendAccumulator = new TokenTrendAccumulator();
        tokenTrendAccumulator.setToken("token");
        tokenTrendAccumulator.setEntries(2);
        tokenTrendAccumulator.setCount(6);
        tokenTrendAccumulator.setLastCount(4);
        TokenStateAggregator agg = new TokenStateAggregator(0.0001f);
        assertEquals(TokenTrend.State.INCREASING, agg.getResult(tokenTrendAccumulator).getState());
    }

    @org.junit.Test
    public void merge() {
        TokenTrendAccumulator tokenTrendAccumulatorA = new TokenTrendAccumulator();
        tokenTrendAccumulatorA.setToken("tokenA");
        tokenTrendAccumulatorA.setEntries(2);
        tokenTrendAccumulatorA.setCount(4);
        tokenTrendAccumulatorA.setLastCount(3);

        TokenTrendAccumulator tokenTrendAccumulatorB = new TokenTrendAccumulator();
        tokenTrendAccumulatorB.setToken("tokenB");
        tokenTrendAccumulatorB.setEntries(2);
        tokenTrendAccumulatorB.setCount(6);
        tokenTrendAccumulatorB.setLastCount(1);

        TokenStateAggregator agg = new TokenStateAggregator(0.0001f);
        TokenTrendAccumulator result = agg.merge(tokenTrendAccumulatorA, tokenTrendAccumulatorB);
        assertEquals(result.getToken(), "tokenA");
        assertEquals(result.getEntries(), 4);
        assertEquals(result.getCount(), 10);
        assertEquals(result.getLastCount(), 3);
    }
}