package com.cisco.amp.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class TweetJsonMap implements FlatMapFunction<String, String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TweetJsonMap.class);

    private transient ObjectMapper jsonParser;

    @Override
    public void flatMap(String value, Collector<String> out) {
        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }

        try {
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
            if (jsonNode.has("errors")) {
                LOGGER.error("Twitter Error: " + value);
            } else {
                String tweetBody = jsonNode.get("text").asText();
                StringTokenizer tokenizer = new StringTokenizer(tweetBody);

                // split the message
                while (tokenizer.hasMoreTokens()) {
                    String result = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();
                    if (!result.equals("") && isInterestingToken(result)) {
                        out.collect(result);
                    }
                }
                out.collect(tweetBody);
            }
        } catch (Exception exception) {
            LOGGER.error(exception.getMessage());
        }
    }

    private Boolean isInterestingToken(String token) {
        Pattern interestingPattern = Pattern.compile("[a-f0-9]{64}|.*(app.any.run|virustotal.com|github.com)\\/.*|^[a-z]:(\\\\|\\/\\/).*\\w+$|^\\/(\\w+\\/)+.*$", Pattern.CASE_INSENSITIVE);
        Matcher interestingMatcher = interestingPattern.matcher(token);

        return interestingMatcher.matches();
    }
}
