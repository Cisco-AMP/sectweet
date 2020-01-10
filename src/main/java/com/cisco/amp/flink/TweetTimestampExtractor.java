package com.cisco.amp.flink;

import com.cisco.amp.flink.model.Tweet;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TweetTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<Tweet> {

    public TweetTimestampExtractor(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Tweet element) {
        return element.getTimestamp();
    }
}
