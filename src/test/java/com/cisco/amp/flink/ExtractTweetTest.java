package com.cisco.amp.flink;

import com.cisco.amp.flink.model.Tweet;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class ExtractTweetTest {
    @Test
    public void flatMapExtractsTweet() throws IOException {
        Collector<Tweet> mockCollector = mock(Collector.class);
        String validTweet = "{\n" +
            "  \"created_at\": \"Thu Apr 06 15:24:15 +0000 2017\",\n" +
            "  \"id_str\": \"850006245121695744\",\n" +
            "  \"text\": \"1\\/ Today we\\u2019re sharing our vision for the future of the Twitter API platform!\\nhttps:\\/\\/t.co\\/XweGngmxlP\",\n" +
            "  \"user\": {\n" +
            "    \"id\": 2244994945,\n" +
            "    \"name\": \"Twitter Dev\",\n" +
            "    \"screen_name\": \"TwitterDev\",\n" +
            "    \"location\": \"Internet\",\n" +
            "    \"url\": \"https:\\/\\/dev.twitter.com\\/\",\n" +
            "    \"description\": \"Your official source for Twitter Platform news, updates & events. Need technical help? Visit https:\\/\\/twittercommunity.com\\/ \\u2328\\ufe0f #TapIntoTwitter\"\n" +
            "  },\n" +
            "  \"place\": {   \n" +
            "  },\n" +
            "  \"entities\": {\n" +
            "    \"hashtags\": [      \n" +
            "    ],\n" +
            "    \"urls\": [\n" +
            "      {\n" +
            "        \"url\": \"https:\\/\\/t.co\\/XweGngmxlP\",\n" +
            "        \"unwound\": {\n" +
            "          \"url\": \"https:\\/\\/cards.twitter.com\\/cards\\/18ce53wgo4h\\/3xo1c\",\n" +
            "          \"title\": \"Building the Future of the Twitter API Platform\"\n" +
            "        }\n" +
            "      }\n" +
            "    ],\n" +
            "    \"user_mentions\": [     \n" +
            "    ]\n" +
            "  }\n" +
            "}\n";
        ExtractTweet extractTweet = new ExtractTweet();
        extractTweet.flatMap(validTweet, mockCollector);
        ObjectMapper jsonParser = new ObjectMapper();
        verify(mockCollector).collect(new Tweet(jsonParser.readValue(validTweet, JsonNode.class)));
    }

    @Test
    public void flatMapIgnoresInvalidJson() {
        Collector<Tweet> mockCollector = mock(Collector.class);
        ExtractTweet extractTweet = new ExtractTweet();
        extractTweet.flatMap("1234", mockCollector);
        verify(mockCollector, never()).collect(any());
    }
}