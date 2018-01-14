package org.engine.publishdata;

import java.util.List;

import org.apache.kafka.streams.KafkaStreams; 
import org.apache.log4j.Logger;
import org.engine.publishdata.db.EntitiesRepository;
import org.engine.publishdata.db.EntitiesRepositoryRedis;
import org.engine.publishdata.local.Simulator;
import org.engine.publishdata.stream.StreamBuilder;
import org.engine.publishdata.utils.Utils; 

import io.redisearch.Document;

public class Main {  	
	static public boolean testing = false;
	final static public Logger logger = Logger.getLogger(Main.class);
	static {
		Utils.setDebugLevel(logger);
	}

	public static void main(String[] args) {
		logger.debug("KAFKA_ADDRESS::::::::" + System.getenv("KAFKA_ADDRESS"));
		logger.debug("SCHEMA_REGISTRY_ADDRESS::::::::" + System.getenv("SCHEMA_REGISTRY_ADDRESS"));
		logger.debug("SCHEMA_REGISTRY_IDENTITY::::::::" + System.getenv("SCHEMA_REGISTRY_IDENTITY"));
		logger.debug("REDIS_HOST::::::::" + System.getenv("REDIS_HOST"));
		logger.debug("REDIS_PORT::::::::" + System.getenv("REDIS_PORT"));
		logger.debug("DEBUG_LEVEL::::::::" + System.getenv("DEBUG_LEVEL"));
		logger.debug("MODE::::::::" + System.getenv("MODE"));

		if(System.getenv("MODE").equals("PUT")) {			
			logger.debug("PUT");
			StreamBuilder streamBuilder = new StreamBuilder();
			KafkaStreams kafkaStreams = streamBuilder.getStream();

			Runtime.getRuntime().addShutdownHook(new Thread(){
				@Override
				public void run() {
					kafkaStreams.close();
				}
			});        
			kafkaStreams.start();

			if(testing) {
				Simulator.writeData(streamBuilder.getSchemaRegistry());
			}
		}
		else {
			logger.debug("GET");
			EntitiesRepository repository = new EntitiesRepositoryRedis();
			int i = 1;
			while(true) {

				double longitude = 32 + (0.001 *i);
				double latitude = 34 + (0.001 *i);

				List<Document> list = repository.queryAllDocuments(longitude, latitude);
				if(list.size() == 0 ) {
					logger.debug("Not Found entities from redis with longitude <"+longitude+"> and latitude <"+latitude+">");
				}
				else {
					logger.debug("The entities from redis with longitude <"+longitude+"> and latitude <"+latitude+"> are: ");
					for(Document doc : list) {					
						logger.debug(doc.toString());
					}				
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) { 
						e.printStackTrace();
					}
				}
				i++;
			}
		}
	}
}