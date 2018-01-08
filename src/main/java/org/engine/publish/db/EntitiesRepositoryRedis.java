package org.engine.publish.db;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.avro.generic.GenericRecord;

import com.google.gson.JsonObject;

import redis.clients.jedis.exceptions.JedisDataException; 
import io.redisearch.Schema;
import io.redisearch.client.Client;

public class EntitiesRepositoryRedis extends EntitiesRepository {

	private Client client;

	@Override
	protected void init() {
		String redisHost = System.getenv("REDIS_HOST");
		int redisPort = Integer.parseInt(System.getenv("REDIS_PORT"));

		client = new Client("entitiesFeed", redisHost, redisPort);
		try {
			client.dropIndex();
		} catch (JedisDataException e) {          
		}
		Schema sc = new Schema().addGeoField("location");
		client.createIndex(sc, Client.IndexOptions.Default());
	}

	@Override
	public void saveEntity(GenericRecord record) {

		Map<String, Object> fields = new HashMap<>();

		Random random = new Random(); 
		double longitude = 30 + random.nextDouble() * 10;
		double latitude = 30 + random.nextDouble() * 10;
		String entityId =  (String)record.get("externalSystemID").toString();

		fields.put("location", longitude + "," + latitude); // Yup, that's the syntax
		byte[] payload = generatePayload(entityId, longitude, latitude);

		client.addDocument(entityId, 1.0, fields, false, true, payload); 
	}

	private byte[] generatePayload(String entityId, double longitude, double latitude) {
		JsonObject payload = new JsonObject();
		payload.addProperty("id", entityId);
		payload.addProperty("longitude", longitude);
		payload.addProperty("latitude", latitude); 
		payload.addProperty("someData", " ");
		payload.addProperty("action", "update");
		return payload.toString().getBytes(StandardCharsets.UTF_8);
	} 
}
