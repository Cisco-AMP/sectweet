package com.cisco.amp.flink.twitter;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class SecurityEndpointInitializer implements TwitterSource.EndpointInitializer, Serializable {
    private final List<Long> twitterIds;
    private final List<String> twitterTerms;

    public SecurityEndpointInitializer(List<Long> twitterIds, List<String> twitterTerms) {
        this.twitterIds = twitterIds;
        this.twitterTerms = twitterTerms;
    }

    @Override
    public StreamingEndpoint createEndpoint() {
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.followings(twitterIds);
        endpoint.trackTerms(twitterTerms);
        endpoint.languages(Arrays.asList("en"));
        return endpoint;
    }
}
