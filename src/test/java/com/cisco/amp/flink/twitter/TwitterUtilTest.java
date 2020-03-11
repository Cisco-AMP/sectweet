package com.cisco.amp.flink.twitter;

import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Test;
import twitter4j.IDs;
import twitter4j.Twitter;
import twitter4j.TwitterException;

import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TwitterUtilTest {

    @Test
    public void getFollowedUserIds_singlePage() throws TwitterException {
        long[] expectedFollowers = new long[]{1L, 2L, 3L};
        IDs ids = mock(IDs.class);
        when(ids.getIDs()).thenReturn(expectedFollowers);
        when(ids.hasNext()).thenReturn(false);

        Twitter twitter = mock(Twitter.class);
        when(twitter.getFriendsIDs(anyLong())).thenReturn(ids);
        TwitterUtil twitterUtil = spy(new TwitterUtil(ParameterTool.fromArgs(new String[]{})));
        doReturn(twitter).when(twitterUtil).getAuthenticatedTwitter();

        List<Long> followers = twitterUtil.getFollowedUserIds();
        assertEquals(expectedFollowers.length, followers.size());
        for (int i = 0; i < expectedFollowers.length; i++) {
            assertEquals(new Long(expectedFollowers[i]), followers.get(i));
        }
    }

    @Test
    public void getFollowedUserIds_multiplePage() throws TwitterException {
        long[] expectedFollowers1 = new long[]{1L, 2L, 3L};
        long[] expectedFollowers2 = new long[]{4L};
        IDs ids = mock(IDs.class);
        when(ids.getIDs()).thenReturn(expectedFollowers1).thenReturn(expectedFollowers2);
        when(ids.hasNext()).thenReturn(true).thenReturn(false);

        Twitter twitter = mock(Twitter.class);
        when(twitter.getFriendsIDs(anyLong())).thenReturn(ids);
        TwitterUtil twitterUtil = spy(new TwitterUtil(ParameterTool.fromArgs(new String[]{})));
        doReturn(twitter).when(twitterUtil).getAuthenticatedTwitter();

        List<Long> followers = twitterUtil.getFollowedUserIds();
        assertEquals(expectedFollowers1.length + expectedFollowers2.length, followers.size());
        for (int i = 0; i < expectedFollowers1.length; i++) {
            assertEquals(new Long(expectedFollowers1[i]), followers.get(i));
        }
    }

    @Test
    public void getFollowedUserIds_noResults() throws TwitterException {
        IDs ids = mock(IDs.class);
        when(ids.getIDs()).thenReturn(new long[]{});
        when(ids.hasNext()).thenReturn(false);

        Twitter twitter = mock(Twitter.class);
        when(twitter.getFriendsIDs(anyLong())).thenReturn(ids);
        TwitterUtil twitterUtil = spy(new TwitterUtil(ParameterTool.fromArgs(new String[]{})));
        doReturn(twitter).when(twitterUtil).getAuthenticatedTwitter();

        List<Long> followers = twitterUtil.getFollowedUserIds();
        assertEquals(0, followers.size());
    }
}