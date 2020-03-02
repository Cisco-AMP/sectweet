package com.cisco.amp.flink.operators;

import com.cisco.amp.flink.operators.TweetJsonMap;
import org.junit.Test;

import static org.junit.Assert.*;

public class TweetJsonMapTest {

    @Test
    public void isReddit_match() {
        TweetJsonMap tweetJsonMap = new TweetJsonMap();
        assertTrue(tweetJsonMap.isReddit("/r/something"));
    }

    @Test
    public void isReddit_prefixOnly() {
        TweetJsonMap tweetJsonMap = new TweetJsonMap();
        assertFalse(tweetJsonMap.isReddit("/r/is/path"));
    }

    @Test
    public void isReddit_notMatch() {
        TweetJsonMap tweetJsonMap = new TweetJsonMap();
        assertFalse(tweetJsonMap.isReddit("not/r/eddit"));
    }
}