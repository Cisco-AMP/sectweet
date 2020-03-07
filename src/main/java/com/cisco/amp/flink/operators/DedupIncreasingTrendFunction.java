package com.cisco.amp.flink.operators;

import com.cisco.amp.flink.model.TokenTrend;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

/**
 * DedupIncreasingTrendFunction filters out all non-increasing trends and
 * limits the outgoing TokenTrend from being repeated more than once per day.
 */
public class DedupIncreasingTrendFunction extends RichFilterFunction<TokenTrend> {
    private transient MapState<String, Boolean> dedup;

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttlConfig = StateTtlConfig
            .newBuilder(Time.days(1))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build();

        MapStateDescriptor<String, Boolean> descriptor =
            new MapStateDescriptor<>(
                "twitter_dedup",
                TypeInformation.of(new TypeHint<String>() {}),
                TypeInformation.of(new TypeHint<Boolean>() {})
            );
        descriptor.enableTimeToLive(ttlConfig);

        dedup = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public boolean filter(TokenTrend value) throws Exception {
        if (value.getState() == TokenTrend.State.INCREASING) {
            if (!dedup.contains(value.getToken())) {
                dedup.put(value.getToken(), null);
                return true;
            }
        }
        return false;
    }
}
