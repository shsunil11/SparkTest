package com.aaa.sparknrt;

import java.util.*;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import org.apache.spark.streaming.flume.*;
//import org.apache.spark.streaming.flume.sink.*;

//JavaReceiverInputDStream<SparkFlumeEvent>flumeStream =
  //  FlumeUtils.createPollingStream(streamingContext, [sink machine hostname], [sink port]);

public class Test2 {
	public static void main(String args[]) {
		SparkConf conf = new SparkConf();

		
		// Create a StreamingContext with a 1-second batch size from a SparkConf
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				Durations.seconds(10)); // Create a DStream from all the input
										// on port 7777
		
		JavaReceiverInputDStream<SparkFlumeEvent> events = FlumeUtils.createPollingStream(jssc, "localhost", 7777);

		
		//JavaDStream<String> lines = jssc.socketTextStream("localhost", 7777);
		// Filter our DStream for lines with "error"

		// Print out the lines with errors
		//lines.print();
		/*
		lines.count().map(new Function<Long, String>() {
            //@Override
            public String call(Long in) {
                return "Received " + in + " flume events.";
            }
        }).print();
		*/
		// Print all the lines
		
		events.foreachRDD(new Function<JavaRDD<SparkFlumeEvent>, Void>() {
			public Void call(JavaRDD<SparkFlumeEvent> rdd) {
				for (SparkFlumeEvent event : rdd.collect()) {
					byte[] eventBody = event.event().getBody().array();
					Map<CharSequence, CharSequence> emap = event.event().getHeaders();
					System.out.println(emap);
					System.out.println(new String(eventBody));
				}
				return null;
			}
		});

		//lines.print();
		// Start our streaming context and wait for it to "finish"
		jssc.start();
		// Wait for the job to finish
		jssc.awaitTermination();
	}

}
