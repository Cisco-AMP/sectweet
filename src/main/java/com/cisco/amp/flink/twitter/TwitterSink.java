package com.cisco.amp.flink.twitter;

import com.cisco.amp.flink.model.TokenTrend;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Twitter;

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
    public void open(Configuration config) throws Exception {
        LOG.info("Initializing Twitter Streaming API connection");
        twitter = new TwitterUtil(params).getAuthenticatedTwitter();
        LOG.info("Signed in with account id: " + twitter.verifyCredentials().getId());
    }

    @Override
    public void invoke(TokenTrend value, Context context) throws Exception {
        twitter.updateStatus(getStatusString(value));
    }

    String getStatusString(TokenTrend value) {
        return "Potential trending #malware token: " + value.getToken();
    }
}
