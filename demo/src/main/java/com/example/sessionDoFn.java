package com.example;

import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class sessionDoFn extends DoFn<KV<String, String>, Void> {

    @StateId("stateKeyState")
    private final StateSpec<ValueState<String>> stateKey = StateSpecs.value();

    @StateId("sessionMessages")
    private final StateSpec<BagState<JsonObject>> sessionMessages = StateSpecs.bag();

    @TimerId("clearAllState")
    private final TimerSpec clearAllState = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @ProcessElement
    public void processElement(ProcessContext c,
            @StateId("stateKeyState") ValueState<String> stateKeyState,
            @StateId("sessionMessages") BagState<JsonObject> sessionMessages,
            @TimerId("clearAllState") Timer clearAllState) {

        String stateKey = c.element().getKey();
        String dataObject = c.element().getValue();

        if (stateKeyState.read() == null) {
            stateKeyState.write(stateKey);

            Duration clearStatebufferDuration = Duration.standardSeconds(30);
            clearAllState.offset(clearStatebufferDuration).setRelative();
            System.out.println("StateKey added for first time");
        }

        sessionMessages.add(JsonParser.parseString(dataObject).getAsJsonObject());
    }

    @OnTimer("clearAllState")
    public void clearAllStateAndTimer(OnTimerContext context,
            @StateId("stateKeyState") ValueState<String> stateKeyState,
            @StateId("sessionMessages") BagState<JsonObject> sessionMessages,
            @TimerId("clearAllState") Timer clearAllState) {
        System.out.println("After 30 seconds: " + stateKeyState.read());
        System.out.println("Bag: " + sessionMessages.read().toString());
        stateKeyState.clear();
    }
}