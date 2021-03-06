package org.engine.publishdata;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.log4j.Logger; 
import org.engine.publishdata.stream.StreamBuilder;
import org.engine.publishdata.utils.Utils;
import org.engine.publishtodb.example.Queries;
import org.engine.publishtodb.example.Simulator;

public class Main {
	static public boolean testing = false;
	final static private Logger logger = Logger.getLogger(Main.class);
	static {
		Utils.setDebugLevel(logger);
	}

	public static void main(String[] args) {
		logger.debug("KAFKA_ADDRESS::::::::" + System.getenv("KAFKA_ADDRESS"));
		logger.debug("SCHEMA_REGISTRY_ADDRESS::::::::"
				+ System.getenv("SCHEMA_REGISTRY_ADDRESS"));
		logger.debug("SCHEMA_REGISTRY_IDENTITY::::::::"
				+ System.getenv("SCHEMA_REGISTRY_IDENTITY"));
		logger.debug("REDIS_HOST::::::::" + System.getenv("REDIS_HOST"));
		logger.debug("REDIS_PORT::::::::" + System.getenv("REDIS_PORT"));
		logger.debug("DEBUG_LEVEL::::::::" + System.getenv("DEBUG_LEVEL"));
		logger.debug("MODE::::::::" + System.getenv("MODE")); 

		if (System.getenv("MODE").equals("PUT")) {
			logger.debug("PUT");
			StreamBuilder streamBuilder = new StreamBuilder();
			KafkaStreams kafkaStreams = streamBuilder.getStream();

			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					kafkaStreams.close();
				}
			});
			kafkaStreams.start();

			if (testing) {
				Simulator.writeData(streamBuilder.getSchemaRegistry());
			}
		} else {
			logger.debug("GET");
			new Queries().generateDocumentQuery();
		} 
	} 
}