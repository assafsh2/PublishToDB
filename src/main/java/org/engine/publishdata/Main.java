package org.engine.publishdata;

import java.util.List;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.log4j.Logger;
import org.engine.publishdata.db.EntitiesRepository;
import org.engine.publishdata.db.EntitiesRepositoryRedis;
import org.engine.publishdata.local.Simulator;
import org.engine.publishdata.stream.StreamBuilder;
import org.engine.publishdata.utils.Utils;

import com.google.gson.JsonObject; 
import com.google.gson.JsonParser;

import io.redisearch.Document;

import java.nio.charset.StandardCharsets;

public class Main {
	static public boolean testing = false;
	final static public Logger logger = Logger.getLogger(Main.class);
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
			EntitiesRepository repository = new EntitiesRepositoryRedis();
			int i = 1;
			while (true) {
				double longitude = 32 + (0.000001 * i);
				double latitude = 34 + (0.000001 * i);

				List<Document> list = repository.queryAllDocuments(longitude,
						latitude);
				if (list.size() == 0) {
					logger.debug("Not Found entities from redis with longitude <"
							+ longitude + "> and latitude <" + latitude + ">");
				} else {
					logger.debug("\n\n\n\n=============================================================");
					logger.debug("The entities from redis with longitude <"
							+ longitude + "> and latitude <" + latitude
							+ "> are [num=" + list.size() + "]: ");
					for (Document doc : list) { 
						logger.debug(doc.toString());
						JsonParser jsonParser = new JsonParser();
						JsonObject payload = (JsonObject) jsonParser.parse(new String(doc.getPayload(), StandardCharsets.UTF_8));
						logger.debug("Payload ="+payload.toString());
					}
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				i++;
				if (longitude > 33 || latitude > 34) {
					i = 0;
				}
			}
		}

	}

}