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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.StringTokenizer;

public class SecTweet {

    private static final String PARAM_FILE_KEY = "file-source";

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // set up the streaming environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Get input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        env.setParallelism(params.getInt("parallelism", 1));

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
            streamSource = env.addSource(twitterSource);
        }

        DataStream<Tuple2<String, Integer>> tweets = streamSource
                // selecting English tweets and splitting to (word, 1)
                .flatMap(new SelectEnglishAndTokenizeFlatMap())
                // group by words and sum their occurrences
                .keyBy(0).sum(1);

        // emit result
        if (params.has("output")) {
            tweets.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            tweets.print();
        }
        env.execute("Sectweet");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * Deserialize JSON from twitter source
     *
     * <p>
     * Implements a string tokenizer that splits sentences into words as a
     * user-defined FlatMapFunction. The function takes a line (String) and splits
     * it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
     * Integer>}).
     */
    public static class SelectEnglishAndTokenizeFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        private transient ObjectMapper jsonParser;

        /**
         * Select the language from the incoming JSON text.
         */
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
            boolean hasText = jsonNode.has("text");
            if (hasText) {
                // message of tweet
                StringTokenizer tokenizer = new StringTokenizer(jsonNode.get("text").asText());

                // split the message
                while (tokenizer.hasMoreTokens()) {
                    String result = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();

                    if (!result.equals("")) {
                        out.collect(new Tuple2<>(result, 1));
                    }
                }
            }
        }
    }
}
