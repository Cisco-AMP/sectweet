package com.cisco.amp.flink.operators;

import com.cisco.amp.flink.model.TokenTrend;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.configuration.Configuration;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class DedupIncreasingTrendFunctionTest {
    TokenTrend tokenTrend = new TokenTrend("token", TokenTrend.State.INCREASING);

    RuntimeContext mockRuntimeContext;
    MapState dedup;
    DedupIncreasingTrendFunction filterFunction;

    @Before
    public void setup() {
        filterFunction = spy(new DedupIncreasingTrendFunction());
        mockRuntimeContext = mock(RuntimeContext.class);
        dedup = mock(MapState.class);
        doReturn(mockRuntimeContext).when(filterFunction).getRuntimeContext();
        when(mockRuntimeContext.getMapState(any())).thenReturn(dedup);
    }

    @Test
    public void invoke_writesNewEntry() throws Exception {
        when(dedup.contains(tokenTrend.getToken())).thenReturn(false);
        when(mockRuntimeContext.getMapState(any())).thenReturn(dedup);

        filterFunction.open(new Configuration());
        assertTrue(filterFunction.filter(tokenTrend));

        verify(dedup).put(eq(tokenTrend.getToken()), any());
    }

    @Test
    public void invoke_dedupsEntry() throws Exception {
        when(dedup.contains(tokenTrend.getToken())).thenReturn(true);

        filterFunction.open(new Configuration());
        assertFalse(filterFunction.filter(tokenTrend));

        verify(dedup, never()).put(eq(tokenTrend.getToken()), any());
    }
}