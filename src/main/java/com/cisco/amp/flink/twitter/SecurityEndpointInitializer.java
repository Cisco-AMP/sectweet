package com.cisco.amp.flink.twitter;

import com.cisco.amp.flink.SecTweet;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import twitter4j.TwitterException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SecurityEndpointInitializer implements TwitterSource.EndpointInitializer, Serializable {
    private final List<Long> twitterIds;
    private final List<String> twitterTerms;

    public SecurityEndpointInitializer(ParameterTool params) throws TwitterException {
        if (params.has(SecTweet.PARAM_TWITTER_SOURCE_IDS_KEY)) {
            twitterIds = getUserIdsFromConfig(params);
        } else {
            twitterIds = new TwitterUtil(params).getFollowedUserIds();
        }
        List<String> twitterTerms = new ArrayList<>();
        for (String term : params.get(SecTweet.PARAM_TWITTER_SOURCE_TERMS_KEY).split(",")) {
            twitterTerms.add(term.trim());
        }
        this.twitterTerms = twitterTerms;
    }

    @Override
    public StreamingEndpoint createEndpoint() {
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.followings(twitterIds);
        endpoint.trackTerms(twitterTerms);
        endpoint.languages(Collections.singletonList("en"));
        return endpoint;
    }

    List<Long> getUserIdsFromConfig(ParameterTool params) {
        List<Long> twitterIds = new ArrayList<>();
        for (String id : params.get(SecTweet.PARAM_TWITTER_SOURCE_IDS_KEY).split(",")) {
            twitterIds.add(Long.parseLong(id.trim()));
        }
        return twitterIds;
    }
}
