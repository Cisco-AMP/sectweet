package com.cisco.amp.flink.operators;

import com.cisco.amp.flink.model.TokenCount;
import com.cisco.amp.flink.model.Tweet;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class TweetJsonMap implements FlatMapFunction<Tweet, TokenCount> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TweetJsonMap.class);
    private static final int MIN_TOKEN_LENGTH = 5;
    private static final Pattern INTERESTING_PATTERN = Pattern.compile("[a-f0-9]{64}|.*(app.any.run|virustotal.com|github.com)\\/.*|^[a-z]:(\\\\|\\/\\/).*\\w+$|^(?!\\/u\\/.*)\\/(\\w+\\/)+.*$");
    private static final List<Character> IGNORE_PREFIX = Arrays.asList('#', '@');

    @Override
    public void flatMap(Tweet tweet, Collector<TokenCount> out) {
        try {
            StringTokenizer tokenizer = new StringTokenizer(tweet.getBody());

            // split the message
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken().toLowerCase();
                if (isInterestingToken(token) && isShaOrUri(token) && !isReddit(token)) {
                    out.collect(new TokenCount(token, 1, tweet.getTimestamp()));
                }
            }
        } catch (Exception exception) {
            LOGGER.error(exception.getMessage());
        }
    }

    boolean isInterestingToken(String token) {
        return token.length() >= MIN_TOKEN_LENGTH
            && !IGNORE_PREFIX.contains(token.charAt(0));
    }

    boolean isShaOrUri(String token) {
        Matcher interestingMatcher = INTERESTING_PATTERN.matcher(token);
        return interestingMatcher.matches();
    }

    boolean isReddit(String token) {
        return token.startsWith("/r/") && token.lastIndexOf("/") == 2;
    }
}
