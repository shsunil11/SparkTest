package com.aaa.sparknrt;

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

public class Test1 {
	public static void main(String args[]) {
		SparkConf conf = new SparkConf();

		// Create a StreamingContext with a 1-second batch size from a SparkConf
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				Durations.seconds(10)); // Create a DStream from all the input
										// on port 7777
		JavaDStream<String> lines = jssc.socketTextStream("localhost", 7777);
		// Filter our DStream for lines with "error"
		JavaDStream<String> errorLines = lines
				.filter(new Function<String, Boolean>() {
					public Boolean call(String line) {
						return line.contains("error");
					}
				});
		// Print out the lines with errors
		errorLines.print();
		// Print all the lines
		lines.foreachRDD(new Function<JavaRDD<String>, Void>() {
			public Void call(JavaRDD<String> rdd) {
				for (String lines : rdd.collect()) {
					System.out.println(lines);
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
