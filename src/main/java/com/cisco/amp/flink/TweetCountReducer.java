package com.cisco.amp.flink;

import com.cisco.amp.flink.model.TokenCount;
import org.apache.flink.api.common.functions.ReduceFunction;

public class TweetCountReducer implements ReduceFunction<TokenCount> {

    @Override
    public TokenCount reduce(TokenCount value1, TokenCount value2) throws Exception {
        if (value1.getToken().equals(value2.getToken())) {
            return new TokenCount(value1.getToken(), value1.getCount() + value2.getCount(), Math.min(value1.getTimestamp(), value2.getTimestamp()));
        } else {
            throw new Exception(String.format("Expected token pairs %s, but received %s %s", value1.getToken(), value1.getToken(), value2.getToken() ));
        }
    }
}
