package com.cisco.amp.flink;

import com.cisco.amp.flink.model.TokenCount;
import com.cisco.amp.flink.model.TokenTrend;
import org.apache.flink.api.common.functions.AggregateFunction;

public class TokenStateAggregator implements AggregateFunction<TokenCount, TokenTrend, TokenTrend.State> {
    private final float equalityRange;

    public TokenStateAggregator(float equalityRange) {
        this.equalityRange = equalityRange;
    }

    @Override
    public TokenTrend createAccumulator() {
        return new TokenTrend();
    }

    @Override
    public TokenTrend add(TokenCount value, TokenTrend accumulator) {
        if (accumulator.getToken() == null)
            accumulator.setToken(value.getToken());
        accumulator.setCount(accumulator.getCount() + value.getCount());
        accumulator.setEntries(accumulator.getEntries() + 1);
        accumulator.setLastCount(value.getCount());
        return accumulator;
    }

    @Override
    public TokenTrend.State getResult(TokenTrend accumulator) {
        float expectedCount = new Float(accumulator.getCount()) / accumulator.getEntries();
        float percentageChange = (accumulator.getLastCount() - expectedCount) / expectedCount;
        System.out.println(percentageChange);
        if (Math.abs(percentageChange) < equalityRange) {
            return TokenTrend.State.NO_CHANGE;
        } else if (percentageChange > 0) {
            return TokenTrend.State.INCREASING;
        } else {
            return TokenTrend.State.DECREASING;
        }
    }

    @Override
    public TokenTrend merge(TokenTrend a, TokenTrend b) {
        TokenTrend mergedTrend = new TokenTrend();
        mergedTrend.setToken(a.getToken());
        mergedTrend.setLastCount(a.getLastCount());
        mergedTrend.setEntries(a.getEntries() + b.getEntries());
        mergedTrend.setCount(a.getCount() + b.getCount());
        return mergedTrend;
    }
}
