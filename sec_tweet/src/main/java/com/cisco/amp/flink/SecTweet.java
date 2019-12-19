/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cisco.amp.flink;

import com.cisco.amp.flink.model.Tweet;
import com.cisco.amp.flink.twitter.SecurityEndpointInitializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

public class SecTweet {
    private static final String PARAM_FILE_KEY = "file-source";
    private static final int MAX_LATENESS_SECONDS = 60;

    public static void main(String[] args) throws Exception {
        // set up the streaming environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Get input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        env.setParallelism(params.getInt("parallelism", 1));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> streamSource;
        if (params.has(PARAM_FILE_KEY)) {
            streamSource = env.readTextFile(params.get(PARAM_FILE_KEY));
        } else {
            if (!(params.has(TwitterSource.CONSUMER_KEY) && params.has(TwitterSource.CONSUMER_SECRET)
                    && params.has(TwitterSource.TOKEN) && params.has(TwitterSource.TOKEN_SECRET))) {
                System.out.println("Usage: --twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> "
                        + "twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret>");
                return;
            }
            // Get input data
            TwitterSource twitterSource = new TwitterSource(params.getProperties());
            twitterSource.setCustomEndpointInitializer(new SecurityEndpointInitializer());
            streamSource = env.addSource(twitterSource);
        }

        DataStream<Tweet> tweets = streamSource
            .flatMap(new ExtractTweet())
            .assignTimestampsAndWatermarks(new TweetTimestampExtractor(Time.seconds(MAX_LATENESS_SECONDS)));

        DataStream<String> tokens = tweets.flatMap(new TweetJsonMap());

        tokens.print();
        env.execute("Sectweet");
    }
}
