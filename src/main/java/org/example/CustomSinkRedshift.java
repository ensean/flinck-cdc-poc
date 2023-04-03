package org.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class CustomSinkRedshift extends RichSinkFunction<String> {

    @Override
    public void invoke(String value, Context context) throws Exception {
        System.out.println(value);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
