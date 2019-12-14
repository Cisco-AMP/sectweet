package com.cisco.amp.flink.twitter;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class SecurityEndpointInitializer implements TwitterSource.EndpointInitializer, Serializable {
    public static final List<Long> twitterIds = Arrays.asList(
        new Long[]{
            2847021941L,               // malwrhunterteam
            370060032L,                // Oddvarmoe
            967487209535361025L,       // DissectMalware
            4302640359L,               // HazMalware
            376580061L,                // makflwana
            239952356L,                // enigma0x3
            3433210978L,               // JAMESWT_MHT
            1590754944L,               // hasherezade
        }
    );

    public static final List<String> terms = Arrays.asList(
        new String[]{
            "infosec",
            "malware"
        }
    );

    @Override
    public StreamingEndpoint createEndpoint() {
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.followings(twitterIds);
        endpoint.trackTerms(terms);
        endpoint.languages(Arrays.asList("en"));
        return endpoint;
    }
}
