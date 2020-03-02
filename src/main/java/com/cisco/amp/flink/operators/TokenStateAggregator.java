package com.cisco.amp.flink.operators;

import com.cisco.amp.flink.model.TokenCount;
import com.cisco.amp.flink.model.TokenTrend;
import com.cisco.amp.flink.model.TokenTrendAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;

public class TokenStateAggregator implements AggregateFunction<TokenCount, TokenTrendAccumulator, TokenTrend> {
    private final float equalityRange;

    public TokenStateAggregator(float equalityRange) {
        this.equalityRange = equalityRange;
    }

    @Override
    public TokenTrendAccumulator createAccumulator() {
        return new TokenTrendAccumulator();
    }

    @Override
    public TokenTrendAccumulator add(TokenCount value, TokenTrendAccumulator accumulator) {
        if (accumulator.getToken() == null)
            accumulator.setToken(value.getToken());
        accumulator.setCount(accumulator.getCount() + value.getCount());
        accumulator.setEntries(accumulator.getEntries() + 1);
        accumulator.setLastCount(value.getCount());
        return accumulator;
    }

    @Override
    public TokenTrend getResult(TokenTrendAccumulator accumulator) {
        if (accumulator.getEntries() == 1) {
            return new TokenTrend(accumulator.getToken(), TokenTrend.State.INCREASING);
        }
        float expectedCount = new Float(accumulator.getCount()) / accumulator.getEntries();
        float percentageChange = (accumulator.getLastCount() - expectedCount) / expectedCount;
        if (Math.abs(percentageChange) < equalityRange) {
            return new TokenTrend(accumulator.getToken(), TokenTrend.State.NO_CHANGE);
        } else if (percentageChange > 0) {
            return new TokenTrend(accumulator.getToken(), TokenTrend.State.INCREASING);
        } else {
            return new TokenTrend(accumulator.getToken(), TokenTrend.State.DECREASING);
        }
    }

    @Override
    public TokenTrendAccumulator merge(TokenTrendAccumulator a, TokenTrendAccumulator b) {
        TokenTrendAccumulator mergedTrend = new TokenTrendAccumulator();
        mergedTrend.setToken(a.getToken());
        mergedTrend.setLastCount(a.getLastCount());
        mergedTrend.setEntries(a.getEntries() + b.getEntries());
        mergedTrend.setCount(a.getCount() + b.getCount());
        return mergedTrend;
    }
}
