package org.engine.publishdata.db;

import java.nio.charset.StandardCharsets; 
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map; 

import org.apache.avro.generic.GenericRecord;  
import org.apache.log4j.Logger;
import org.engine.publishdata.utils.Utils;  

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisDataException;

import com.google.gson.JsonObject; 

import io.redisearch.Query;  
import io.redisearch.Schema;
import io.redisearch.SearchResult;
import io.redisearch.client.Client;
import io.redisearch.Document;

public class EntitiesRepositoryRedis implements EntitiesRepository {
	final static private Logger logger = Logger.getLogger(EntitiesRepositoryRedis.class);
	static {
		Utils.setDebugLevel(logger);
	}	
	private io.redisearch.client.Client client; 
	private redis.clients.jedis.Jedis jedis;

	public EntitiesRepositoryRedis() {
		String redisHost = System.getenv("REDIS_HOST");
		int redisPort = Integer.parseInt(System.getenv("REDIS_PORT"));

		client = new Client("entitiesFeed", redisHost, redisPort); 	 
 
		try {
			client.dropIndex();
		} catch (JedisDataException e) {          
		}
		Schema sc = new Schema().addGeoField("location");
		client.createIndex(sc, Client.IndexOptions.Default()); 
		
		
		jedis = new  Jedis(redisHost, redisPort); 
		
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
		//fields.put("entityId", entityId);
		byte[] payload = generatePayload(entityId, longitude, latitude,sourceName,metadata); 

		boolean status = client.addDocument(entityId, 1.0, fields, false, true, payload);
 
		logger.debug("addDocument ended with <"+status+">"); 

		
		//logger.debug("Query by entityId:"+entityId+" retrive record: "+queryDocumentByEntityId(entityId));
	}

 
	public void test(int i) {
		Map<String, Object> fields = new HashMap<>();

		String entityId = "id"+i;
		double longitude = 10.2 + (0.001*i);
		double latitude = 20.2 + (0.001*i);

		fields.put("location", longitude + "," + latitude);  
		byte[] payload = generatePayload(entityId, longitude, latitude,"source","metadata"+i); 

		boolean status = client.addDocument(entityId, 1.0, fields, false, true, payload);
 
		logger.debug("addDocument ended with <"+status+">"); 
		
	    Pipeline pipeline = jedis.pipelined();
	 //   logger.debug("Return - "+jedis.eval("FT.GET", Arrays.asList("entitiesFeed"), Arrays.asList(entityId)).toString();
	    
	    /*REDIS
	    Response<Map<String, String>> bhashResponse = pipeline.hgetAll(entityId);
	    pipeline.sync();
	    Map<String, String> bhash = bhashResponse.get();
		logger.debug("Query by entityId:"+entityId+" retrive record: "+bhash);
		
		FT.SEARCH entitiesFeed '@location:[34 32 100000 km]'  INKEYS 1 97f6de16-7f70-4fb0-93e2-f57fd4abada4  WITHPAYLOADS
		 */
		String queryString = "@location:[" + longitude
				+ " " + latitude
				+ " 100000000 km] INKEYS 1 "+entityId+" ";
		Query query = new Query(queryString).setWithPaload().limit(0,1000000); 

		logger.debug("Return - "+ client.search(query).docs);
	     
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
