package org.engine.publishdata.db;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map; 

import org.apache.avro.generic.GenericRecord;  
import org.apache.log4j.Logger;
import org.engine.publishdata.utils.Utils; 

import com.google.gson.JsonObject; 

import io.redisearch.Query;    
import io.redisearch.SearchResult;
import io.redisearch.client.Client;
import io.redisearch.Document;

public class EntitiesRepositoryRedis implements EntitiesRepository {
	final static private Logger logger = Logger.getLogger(EntitiesRepositoryRedis.class);
	static {
		Utils.setDebugLevel(logger);
	}	
	private Client client; 

	public EntitiesRepositoryRedis() {
		String redisHost = System.getenv("REDIS_HOST");
		int redisPort = Integer.parseInt(System.getenv("REDIS_PORT"));

		client = new Client("entitiesFeed", redisHost, redisPort); 	 
		/* 
		try {
			client.dropIndex();
		} catch (JedisDataException e) {          
		}
		Schema sc = new Schema().addGeoField("location");
		client.createIndex(sc, Client.IndexOptions.Default());*/ 
	}

	@Override
	public void saveEntity(GenericRecord record) {
		Map<String, Object> fields = new HashMap<>();

		String entityId = (String) record.get("entityID").toString();
		GenericRecord entityAttributes = (GenericRecord)record.get("entityAttributes");
		String metadata = (String) entityAttributes.get("metadata").toString();
		GenericRecord basicAttributes = (GenericRecord)entityAttributes.get("basicAttributes");
		String sourceName = (String) basicAttributes.get("sourceName").toString();
		GenericRecord coordinate = (GenericRecord) basicAttributes.get("coordinate"); 
		double longitude = (double) coordinate.get("long");
		double latitude = (double) coordinate.get("lat"); 

		fields.put("location", longitude + "," + latitude);  
		fields.put("entityId", entityId);
		byte[] payload = generatePayload(entityId, longitude, latitude,sourceName,metadata); 

		client.addDocument(entityId, 1.0, fields, false, true, payload); 
		
		logger.debug("Query by entityId:"+entityId+" retrive record: "+queryDocumentByEntityId(entityId));
	}

	@Override
	public List<Document> queryDocumentsByGeo(double longitude, double latitude ) { 
		String queryString = "@location:[" + longitude
				+ " " + latitude
				+ " 10 km]";
		Query query = new Query(queryString).setWithPaload().limit(0,1000000); 

		return client.search(query).docs;
	}

	@Override
	public Document queryDocumentByEntityId(String entityId) { 
		String queryString = "@entityId:"+entityId;
		Query query = new Query(queryString).setWithPaload().limit(0,1000000);
		SearchResult result = client.search(query); 
		return result.totalResults == 0 ? null : result.docs.get(0);
	} 

	private byte[] generatePayload(String entityId, double longitude, double latitude,String sourceName, String metadata) {
		JsonObject payload = new JsonObject();
		payload.addProperty("id", entityId);
		payload.addProperty("longitude", longitude);
		payload.addProperty("latitude", latitude); 
		payload.addProperty("sourceName",sourceName);
		payload.addProperty("metadata",metadata);
		payload.addProperty("action", "update");	 

		return payload.toString().getBytes(StandardCharsets.UTF_8);
	}  
}
