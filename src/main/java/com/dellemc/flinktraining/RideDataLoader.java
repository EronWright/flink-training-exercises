/*
 * Copyright 2018 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dellemc.flinktraining;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.serialization.PravegaSerialization;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RideDataLoader extends ExerciseBase {
	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", pathToRideData);

		PravegaConfig pravegaConfig = PravegaConfig
				.fromParams(params)
				.withDefaultScope(params.get("scope", "project1"));

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// prepare Pravega stream
		String scopeName = pravegaConfig.getDefaultScope();
		String streamName = params.get("stream", "ridedata");
		try(StreamManager streamManager = StreamManager.create(pravegaConfig.getClientConfig())) {
			// create the requested scope (if necessary)
			streamManager.createScope(scopeName);
			streamManager.createStream(scopeName, streamName, StreamConfiguration.builder()
					.scalingPolicy(ScalingPolicy.fixed(1)).build());
		}

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor));

		// write to Pravega stream

		FlinkPravegaWriter<TaxiRide> writer = FlinkPravegaWriter.<TaxiRide>builder()
				.withPravegaConfig(pravegaConfig)
				.forStream(streamName)
				.withEventRouter(ride -> Long.toString(ride.rideId))
				.withSerializationSchema(PravegaSerialization.serializationFor(TaxiRide.class))
				.build();

		rides.addSink(writer);
		rides.print();

		// run the cleansing pipeline
		env.execute("Taxi Ride Data Loader");
	}

	public static class NYCFilter implements FilterFunction<TaxiRide> {
		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {

			return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
					GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
		}
	}
}
