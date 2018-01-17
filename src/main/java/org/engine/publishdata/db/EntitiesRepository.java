package org.engine.publishdata.db;

import io.redisearch.Document;

import java.util.List;

import org.apache.avro.generic.GenericRecord;

public interface EntitiesRepository {
	
	void saveEntity(GenericRecord record);	 

	List<Document> queryDocumentsByGeo(double longitude, double latitude); 
	
	Document queryDocumentByEntityId(String entityId);
}
