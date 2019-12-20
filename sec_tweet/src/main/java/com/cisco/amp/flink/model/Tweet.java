package com.cisco.amp.flink.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

public class Tweet {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter
        .ofPattern("EEE MMM dd HH:mm:ss xxxx yyyy");
    private final long timestamp;
    private final String body;

    public Tweet(JsonNode jsonNode) {
        this(
            Tweet.getTimestamp(jsonNode.get("created_at").asText()),
            jsonNode.get("text").asText()
        );
//        System.out.println(timestamp + ": " + body);
    }

    public Tweet(long timestamp, String body) {
        this.timestamp = timestamp;
        this.body = body;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getBody() {
        return body;
    }

    private static long getTimestamp(String timeString) {
        OffsetDateTime odtInstanceAtOffset = OffsetDateTime.parse(timeString, DATE_TIME_FORMATTER);
        return odtInstanceAtOffset.toInstant().getEpochSecond() * 1000;
    }
}
