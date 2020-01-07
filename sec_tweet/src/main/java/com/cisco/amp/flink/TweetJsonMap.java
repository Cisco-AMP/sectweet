package com.cisco.amp.flink;

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
    private static final Pattern interestingPattern = Pattern.compile("[a-f0-9]{64}|.*(app.any.run|virustotal.com|github.com)\\/.*|^[a-z]:(\\\\|\\/\\/).*\\w+$|^\\/(\\w+\\/)+.*$");
    private static final List<Character> IGNORE_PREFIX = Arrays.asList(new Character[]{'#', '@'});

    @Override
    public void flatMap(Tweet tweet, Collector<TokenCount> out) {
        try {
            StringTokenizer tokenizer = new StringTokenizer(tweet.getBody());

            // split the message
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken().toLowerCase();
                if (isInterestingToken(token) && isShaOrUri(token)) {
                    out.collect(new TokenCount(token, 1));
                }
            }
        } catch (Exception exception) {
            LOGGER.error(exception.getMessage());
        }
    }

    private boolean isInterestingToken(String token) {
        return token.length() >= MIN_TOKEN_LENGTH
            && !IGNORE_PREFIX.contains(token.charAt(0));
    }

    private boolean isShaOrUri(String token) {
        Matcher interestingMatcher = interestingPattern.matcher(token);
        return interestingMatcher.matches();
    }
}
