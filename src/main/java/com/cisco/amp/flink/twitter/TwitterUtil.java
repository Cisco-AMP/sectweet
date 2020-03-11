package com.cisco.amp.flink.twitter;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import twitter4j.IDs;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;

import java.util.ArrayList;
import java.util.List;

public class TwitterUtil {
    private final ParameterTool params;

    public TwitterUtil(ParameterTool params) {
        this.params = params;
    }

    Twitter getAuthenticatedTwitter() {
        Twitter twitter = new TwitterFactory().getInstance();
        twitter.setOAuthConsumer(params.get(TwitterSource.CONSUMER_KEY), params.get(TwitterSource.CONSUMER_SECRET));
        twitter.setOAuthAccessToken(new AccessToken(params.get(TwitterSource.TOKEN), params.get(TwitterSource.TOKEN_SECRET)));
        return twitter;
    }

    List<Long> getFollowedUserIds() throws TwitterException {
        List<Long> twitterIds = new ArrayList<>();
        Twitter twitter = getAuthenticatedTwitter();
        IDs ids;
        long cursor = -1;
        do {
            ids = twitter.getFriendsIDs(cursor);
            for (long id : ids.getIDs()) {
                twitterIds.add(id);
            }
            cursor = ids.getNextCursor();
        } while (ids.hasNext());
        return twitterIds;
    }
}
