package com.cisco.amp.flink.twitter;

import com.cisco.amp.flink.model.TokenTrend;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;

import static org.apache.flink.streaming.connectors.twitter.TwitterSource.*;

public class TwitterSink extends RichSinkFunction<TokenTrend> {
    private static final Logger LOG = LoggerFactory.getLogger(TwitterSink.class);
    private static final long serialVersionUID = 1L;
    private final ParameterTool params;

    // ----- Runtime fields
    private transient Twitter twitter;

    public TwitterSink(ParameterTool params) {
        this.params = params;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        twitter = getAuthenticatedTwitter(params);
    }

    @Override
    public void invoke(TokenTrend value, Context context) throws Exception {
        twitter.updateStatus(getStatusString(value));
    }

    String getStatusString(TokenTrend value) {
        return "Trending token: " + value.getToken();
    }

    Twitter getAuthenticatedTwitter(ParameterTool params) throws TwitterException {
        LOG.info("Initializing Twitter Streaming API connection");
        Twitter twitter = new TwitterFactory().getInstance();
        twitter.setOAuthConsumer(params.get(CONSUMER_KEY), params.get(CONSUMER_SECRET));
        twitter.setOAuthAccessToken(new AccessToken(params.get(TOKEN), params.get(TOKEN_SECRET)));
        LOG.info("Signed in with account id: " + twitter.verifyCredentials().getId());
        return twitter;
    }
}
