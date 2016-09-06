package org.stsffap;

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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;


public class StreamingJob {

	public static void main(String[] args) throws Exception {
		final int groups = 10;
		ParameterTool parameterTool = ParameterTool.fromArgs(args);

		final String topic = parameterTool.get("topic", "input");
		final Properties props = new Properties();

		final String bootstrapServers = parameterTool.get("bootstrapServers", "localhost:9092");
		final String groupId = parameterTool.get("groupId", "default");

		props.setProperty("bootstrap.servers", bootstrapServers);
		props.setProperty("group.id", groupId);



		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> input = env.addSource(new FlinkKafkaConsumer09<>(topic, new SimpleStringSchema(), props));

		DataStream<Integer> integerInput = input.flatMap(new FlatMapFunction<String, Integer>() {
			private static final long serialVersionUID = 1245758969199704087L;

			@Override
			public void flatMap(String s, Collector<Integer> collector) throws Exception {
				try {
					collector.collect(Integer.valueOf(s));
				} catch (NumberFormatException e) {
					// ignore
				}
			}
		});

		DataStream<Tuple2<Integer, Integer>> result = integerInput.keyBy(new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = -602498589721795420L;

			@Override
			public Integer getKey(Integer integer) throws Exception {
				return integer % groups;
			}
		}).map(new RichMapFunction<Integer, Tuple2<Integer, Integer>>() {
			private static final long serialVersionUID = -8454791371747729272L;

			private final ValueStateDescriptor<Integer> counterStateDescriptor = new ValueStateDescriptor<>("counter", Integer.class, 0);

			private transient ValueState<Integer> counter;

			@Override
			public void open(Configuration configuration) {
				counter = getRuntimeContext().getState(counterStateDescriptor);
			}

			@Override
			public Tuple2<Integer, Integer> map(Integer integer) throws Exception {
				int currentCount = counter.value();

				counter.update(currentCount + 1);

				return Tuple2.of(integer, currentCount + 1);
			}
		});

		result.addSink(new PrintSinkFunction<Tuple2<Integer, Integer>>());

		// execute program
		env.execute("Flink Forward Kafka demo");
	}
}