package com.example;

import com.google.gson.JsonObject;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class JsonObjectCoder extends AtomicCoder<JsonObject> {
    private static final JsonObjectCoder INSTANCE = new JsonObjectCoder();

    public static JsonObjectCoder of() {
        return INSTANCE;
    }

    @Override
    public void encode(JsonObject value, OutputStream outStream) throws CoderException, IOException {
        String jsonString = value.toString();
        StringUtf8Coder.of().encode(jsonString, outStream);
    }

    @Override
    public JsonObject decode(InputStream inStream) throws CoderException, IOException {
        String jsonString = StringUtf8Coder.of().decode(inStream);
        return JsonParser.parseString(jsonString).getAsJsonObject();
    }
}
