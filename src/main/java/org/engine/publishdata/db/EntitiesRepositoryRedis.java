package org.engine.publishdata.db;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map; 

import org.apache.avro.generic.GenericRecord;

import com.google.gson.JsonObject;
 
import io.redisearch.Query; 
import io.redisearch.SearchResult;
import io.redisearch.client.Client;
import io.redisearch.Document;

public class EntitiesRepositoryRedis implements EntitiesRepository {

	private Client client; 

	public EntitiesRepositoryRedis() {
		String redisHost = System.getenv("REDIS_HOST");
		int redisPort = Integer.parseInt(System.getenv("REDIS_PORT"));

		client = new Client("entitiesFeed", redisHost, redisPort); 	
	}

	@Override
	public void saveEntity(GenericRecord record) {
		Map<String, Object> fields = new HashMap<>();

		String entityId = (String) record.get("entityID").toString();
		GenericRecord entityAttributes = (GenericRecord)record.get("entityAttributes");
		GenericRecord basicAttributes = (GenericRecord)entityAttributes.get("basicAttributes");
		String sourceName = (String) basicAttributes.get("sourceName").toString();
		GenericRecord coordinate = (GenericRecord) basicAttributes.get("coordinate"); 
		double longitude = (double) coordinate.get("long");
		double latitude = (double) coordinate.get("lat"); 

		fields.put("location", longitude + "," + latitude);  
		byte[] payload = generatePayload(entityId, longitude, latitude,sourceName); 

		client.addDocument(entityId, 1.0, fields, false, true, payload); 
	}
	
	@Override
	public List<Document> queryDocuments(double longitude, double latitude) {
		List<Document> list = new ArrayList<Document>(); 
		String queryString = "@location:[" + longitude
				+ " " + latitude
				+ " 10 km]";
		Query query = new Query(queryString).setWithPaload().limit(0,Integer.MAX_VALUE);
		SearchResult res = client.search(query);
		list.addAll(res.docs);

		return list;
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
}
