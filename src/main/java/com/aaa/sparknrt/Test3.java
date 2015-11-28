package com.aaa.sparknrt;

import java.util.*;

import org.apache.commons.lang.StringUtils;
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

public class Test3 {

	public static void main(String args[]) {
		
		int batchCount = 0;
		
		SparkConf conf = new SparkConf();

		// Create a StreamingContext with a 1-second batch size from a SparkConf
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				Durations.seconds(10)); // Create a DStream from all the input
										// on port 7777

		JavaReceiverInputDStream<SparkFlumeEvent> events = FlumeUtils
				.createPollingStream(jssc, "localhost", 7777);
		// Print all lines
		
		JavaDStream<String> eventStr = events.map(new Function<SparkFlumeEvent,String>() {
		     public	String call (SparkFlumeEvent e) {
		    	 return new String(e.event().getBody().array());
		    	 
		     }
		});
		
		eventStr.countByValue().print(100);
		
		eventStr.foreachRDD(new Function<JavaRDD<String>, Void>() {
			public Void call(JavaRDD<String> rdd) {
				int i = 0;
				rdd.saveAsTextFile("/user/cloudera/spk_" + System.currentTimeMillis());
				return null;
			}
		});
		
 		events.foreachRDD(new Function<JavaRDD<SparkFlumeEvent>, Void>() {
			public Void call(JavaRDD<SparkFlumeEvent> rdd) {
				int i = 0;
				for (SparkFlumeEvent event : rdd.collect()) {
					byte[] eventBody = event.event().getBody().array();
					Map<CharSequence, CharSequence> emap = event.event()
							.getHeaders();
					System.out.println("Event No = " + (++i) + " " + emap);
					System.out.println(new String(eventBody));
				}
				return null;
			}
		});
	
		System.out.println("Batch No = " + (++batchCount));
	

		// Start our streaming context and wait for it to "finish"
		jssc.start();
		// Wait for the job to finish
		jssc.awaitTermination();
	}

}
