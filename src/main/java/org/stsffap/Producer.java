package org.stsffap;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;
import java.util.Random;

/**
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

public class Producer {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        final int maxValue = parameterTool.getInt("maxValue", 1000);
        final String topic = parameterTool.get("output", "input");
        final String bootstrapServers = parameterTool.get("bootstrapServers", "localhost:9092");
        final long delay = parameterTool.getLong("delay", 100L);
        final Properties props = new Properties();

        props.setProperty("bootstrap.servers", bootstrapServers);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = env
            .addSource(new MyGeneratingSource(maxValue, delay))
            .map(new MapFunction<Integer, String>() {
                private static final long serialVersionUID = -602911330537489999L;

                @Override
                public String map(Integer integer) throws Exception {
                    return String.valueOf(integer);
                }
            });

        input.addSink(new FlinkKafkaProducer09<>(topic, new SimpleStringSchema(), props));

        env.execute("Flink Forward Demo: Kafka Producer");
    }

    private static class MyGeneratingSource implements ParallelSourceFunction<Integer> {

        private static final long serialVersionUID = 7013561753066903029L;

        private final int maxValue;
        private final long delay;
        private final Random random;

        private boolean running = true;

        private MyGeneratingSource(int maxValue, long delay) {
            this.maxValue = maxValue;
            this.delay = delay;

            this.random = new Random();
        }

        @Override
        public void run(SourceContext<Integer> sourceContext) throws Exception {
            while(running) {
                synchronized (sourceContext.getCheckpointLock()) {
                    sourceContext.collect(random.nextInt(maxValue));
                }

                Thread.sleep(delay);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
