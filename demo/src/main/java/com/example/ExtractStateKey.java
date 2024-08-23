package com.example;

import java.nio.charset.StandardCharsets;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ExtractStateKey extends DoFn<PubsubMessage, KV<String, String>> {

    @ProcessElement
    public void publicElement(ProcessContext c) {
        byte[] payload = c.element().getPayload();
        String pubsubMessage = new String(payload, StandardCharsets.UTF_8);
        JsonObject pubsubJsonPayload = JsonParser.parseString(pubsubMessage).getAsJsonObject();

        System.out.println("pubsubJsonPayload: " + pubsubJsonPayload);
        String sessionKey = pubsubJsonPayload.getAsJsonObject("mp").get("hsk").getAsString();

        JsonObject dataObject = new JsonObject();

        String userKey = pubsubJsonPayload.getAsJsonObject("up").get("uk").getAsString();
        String message = pubsubJsonPayload.getAsJsonObject("mp").get("tct").getAsString();
        String messageTimestamp = pubsubJsonPayload.getAsJsonObject("mp").get("ct").getAsString();

        dataObject.addProperty("message", message);
        dataObject.addProperty("userKey", userKey);
        dataObject.addProperty("messageTimestamp", messageTimestamp);

        c.output(KV.of(sessionKey, dataObject.toString()));
    }
}
