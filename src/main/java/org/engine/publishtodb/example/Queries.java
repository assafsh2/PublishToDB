package org.engine.publishtodb.example;

import io.redisearch.Document;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.log4j.Logger;
import org.engine.publishdata.Main;
import org.engine.publishdata.db.EntitiesRepository;
import org.engine.publishdata.db.EntitiesRepositoryRedis;
import org.engine.publishdata.utils.Utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Queries {
	final static private Logger logger = Logger.getLogger(Main.class);
	static {
		Utils.setDebugLevel(logger);
	}	
	
	public void generateDocumentQuery() {
		EntitiesRepository repository = new EntitiesRepositoryRedis();
		int i = 1;
		while (true) {
			double longitude = 32 + (0.0001 * i);
			double latitude = 34 + (0.0001 * i);

			List<Document> list = repository.queryDocuments(longitude,latitude);
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
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			i++;
			if (longitude > 33 || latitude > 35) {
				i = 0;
			}
		}
		
	}
}
