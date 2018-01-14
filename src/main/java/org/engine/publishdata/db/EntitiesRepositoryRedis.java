package org.engine.publishdata.db;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map; 

import org.apache.avro.generic.GenericRecord;



import com.google.gson.JsonObject;

import redis.clients.jedis.exceptions.JedisDataException; 
import io.redisearch.Query;
import io.redisearch.Schema;
import io.redisearch.SearchResult;
import io.redisearch.client.Client;
import io.redisearch.Document;

public class EntitiesRepositoryRedis extends EntitiesRepository {

	private Client client; 

	@Override
	protected void init() {
		String redisHost = System.getenv("REDIS_HOST");
		int redisPort = Integer.parseInt(System.getenv("REDIS_PORT"));

		client = new Client("entitiesFeed", redisHost, redisPort);
		if(System.getenv("MODE").equals("PUT")) {	
			try {
				client.dropIndex();
			} catch (JedisDataException e) {          
			}
			Schema sc = new Schema().addGeoField("location");
			client.createIndex(sc, Client.IndexOptions.Default());
		}
	}

	@Override
	public void saveEntity(GenericRecord record) {
		Map<String, Object> fields = new HashMap<>();

		GenericRecord entityAttributes = (GenericRecord)record.get("entityAttributes");
		String externalSystemID = (String) entityAttributes.get("externalSystemID").toString();
		GenericRecord basicAttributes = (GenericRecord)entityAttributes.get("basicAttributes");
		String sourceName = (String) basicAttributes.get("sourceName").toString();
		GenericRecord coordinate = (GenericRecord) basicAttributes.get("coordinate"); 

		double longitude = (double) coordinate.get("long");
		double latitude = (double) coordinate.get("lat"); 

		fields.put("location", longitude + "," + latitude);  
		byte[] payload = generatePayload(externalSystemID, longitude, latitude,sourceName); 

		client.addDocument(externalSystemID, 1.0, fields, false, true, payload); 
	}

	private byte[] generatePayload(String entityId, double longitude, double latitude,String sourceName) {
		JsonObject payload = new JsonObject();
		payload.addProperty("id", entityId);
		payload.addProperty("longitude", longitude);
		payload.addProperty("latitude", latitude); 
		payload.addProperty("sourceName",sourceName);
		payload.addProperty("action", "update");
		return payload.toString().getBytes(StandardCharsets.UTF_8);
	} 

	public List<Document> queryAllDocuments(double longitude, double latitude) {
		List<Document> list = new ArrayList<Document>(); 
		String queryString = "@location:[" + longitude
				+ " " + latitude
				+ " 1 km]";
		Query query = new Query(queryString).setWithPaload();
		SearchResult res = client.search(query);
		list.addAll(res.docs);

		return list;
	} 
}
