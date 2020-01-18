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
    }

    public Tweet(long timestamp, String body) {
        this.timestamp = timestamp;
        this.body = body;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String getBody() {
        return body;
    }

    private static long getTimestamp(String timeString) {
        OffsetDateTime odtInstanceAtOffset = OffsetDateTime.parse(timeString, DATE_TIME_FORMATTER);
        return odtInstanceAtOffset.toInstant().getEpochSecond() * 1000;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (!(other instanceof Tweet)) {
            return false;
        }

        Tweet otherTweet = (Tweet) other;
        return otherTweet.getTimestamp() == this.timestamp
            && otherTweet.getBody().equals(this.getBody());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
            + ((getBody() == null) ? 0 : getBody().hashCode());
        result = prime * result
            + ((getTimestamp() == null) ? 0 : getTimestamp().hashCode());
        return result;
    }
}
