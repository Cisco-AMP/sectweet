package com.cisco.amp.flink;

import com.cisco.amp.flink.model.Tweet;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtractTweet implements FlatMapFunction<String, Tweet> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractTweet.class);
    private transient ObjectMapper jsonParser;

    @Override
    public void flatMap(String value, Collector<Tweet> out) {
        // Ignore non JSON messages from the Twitter API
        if (!value.startsWith("{")) {
            return;
        }

        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }

        try {
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
            if (jsonNode.has("errors")) {
                LOGGER.error("Twitter Error: " + value);
            } else {
                out.collect(new Tweet(jsonNode));
            }
        } catch (Exception exception) {
            LOGGER.error("Failed to parse twitter message: " + value, exception);
        }
    }
}
