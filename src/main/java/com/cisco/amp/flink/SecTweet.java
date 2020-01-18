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

import com.cisco.amp.flink.model.TokenCount;
import com.cisco.amp.flink.model.TokenTrend;
import com.cisco.amp.flink.model.Tweet;
import com.cisco.amp.flink.twitter.SecurityEndpointInitializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;

import org.apache.flink.util.ExceptionUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.*;

public class SecTweet {
    private static final String PARAM_FILE_KEY = "file-source";
    private static final String PARAM_USE_ES = "use-es";
    private static final int MAX_LATENESS_SECONDS = 60;
    private static final Time DEFAULT_RATE_INTERVAL = Time.minutes(15);
    private static final int DEFAULT_TREND_WINDOW_SIZE = 5;
    private static final int DEFAULT_TREND_WINDOW_SLIDE = 1;
    private static final float TREND_EQUALITY_RANGE = 0.01f;

    void writeToES(DataStream<TokenCount> tokenCountDataStream) {
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));

        ElasticsearchSink.Builder<TokenCount> esSinkBuilder = new ElasticsearchSink.Builder<>(
            httpHosts,
            (TokenCount element, RuntimeContext ctx, RequestIndexer indexer) -> {
                indexer.add(createIndexRequest(element));
            });

        esSinkBuilder.setFailureHandler((ActionRequestFailureHandler) (action, failure, restStatusCode, indexer) -> {
            if (ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()) {
                indexer.add(action);
            } else {
                throw failure;
            }
        });
        // this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);
        tokenCountDataStream.addSink(esSinkBuilder.build());
    }

    void buildJobGraph(StreamExecutionEnvironment env, ParameterTool params) {
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

        DataStream<TokenCount> tokens = tweets
                .flatMap(new TweetJsonMap())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TokenCount>() {
                    @Override
                    public long extractAscendingTimestamp(TokenCount element) { return element.getTimestamp(); }
                });
        DataStream<TokenCount> tokenCountDataStream = countTokens(tokens, DEFAULT_RATE_INTERVAL);
        DataStream<TokenTrend> trendsDataStream = getTrends(tokenCountDataStream, DEFAULT_TREND_WINDOW_SIZE, DEFAULT_TREND_WINDOW_SLIDE);

        if (params.has(PARAM_USE_ES)) {
            writeToES(tokenCountDataStream);
        }
        trendsDataStream.print();
    }

    DataStream<TokenCount> countTokens(DataStream<TokenCount> dataStream, Time windowSize) {
        DataStream<TokenCount> tokenCountDataStream = dataStream.keyBy("token")
            .window(TumblingEventTimeWindows.of(windowSize))
            .reduce(new TweetCountReducer());
        return tokenCountDataStream;
    }

    DataStream<TokenTrend> getTrends(DataStream<TokenCount> dataStream, int windowSize, int windowSlide) {
        DataStream<TokenTrend> tokenTrendDataStream = dataStream
            .keyBy("token")
            .countWindow(windowSize, windowSlide)
            .aggregate(new TokenStateAggregator(TREND_EQUALITY_RANGE));
        return tokenTrendDataStream;
    }

    public static void main(String[] args) throws Exception {
        // set up the streaming environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Get input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        env.setParallelism(params.getInt("parallelism", 1));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SecTweet secTweet = new SecTweet();
        secTweet.buildJobGraph(env, params);

        env.execute("Sectweet");
    }

    private static IndexRequest createIndexRequest(TokenCount element) {
        Map<String, Object> json = new HashMap<>();
        json.put("token", element.getToken());
        json.put("count", element.getCount());
        json.put("timestamp", new Date(element.getTimestamp()));

        return Requests.indexRequest()
            .index("sectweet")
            .source(json)
            .type("tokenCount");
    }
}
