package com.example;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.google.gson.JsonObject;

public class App {

    public static void runPipeline(DataflowPipelineOptions options) {
        Pipeline p = Pipeline.create(options);
        p.getCoderRegistry().registerCoderProvider(
                CoderProviders.fromStaticMethods(JsonObject.class, JsonObjectCoder.class));

        PCollection<KV<String, String>> pubSubData = p.apply("Reading data from PubSub",
                PubsubIO.readMessagesWithAttributes()
                        .fromSubscription("projects/weheal-debug/subscriptions/messages-test"))
                .apply(ParDo.of(new ExtractStateKey()));

        pubSubData.apply("Sending data to session wise state", ParDo.of(new sessionDoFn()));

        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        runPipeline(options);
    }
}