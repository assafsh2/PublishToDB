package org.engine.publishtodb.example;

import io.redisearch.Document;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.log4j.Logger;
import org.engine.publishdata.db.EntitiesRepository;
import org.engine.publishdata.db.EntitiesRepositoryRedis;
import org.engine.publishdata.utils.Utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Queries {
	final static private Logger logger = Logger.getLogger(Queries.class);
	static {
		Utils.setDebugLevel(logger);
	}

	public void generateDocumentQuery() {
		EntitiesRepository repository = new EntitiesRepositoryRedis();
		int i = 0;
		int j = 0;
		double longitude = 0;
		double latitude = 0;
		while (true) {
			latitude = 32.425 + (0.001 * i);
			j = 0;
			do {
				longitude = 34.554 + (0.001 * j);

				List<Document> list = repository.queryDocumentsByGeo(longitude,
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
						JsonObject payload = (JsonObject) jsonParser
								.parse(new String(doc.getPayload(),
										StandardCharsets.UTF_8));
						logger.debug("Payload =" + payload.toString());
					}
					try {
						Thread.sleep(5);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

				j++;

			} while (latitude < 35);

			i++;
			if (longitude > 33) {
				i = 0;
			}
		}

	}
}
