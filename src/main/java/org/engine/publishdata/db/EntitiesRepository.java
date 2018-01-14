package org.engine.publishdata.db;

import io.redisearch.Document;

import java.util.List;

import org.apache.avro.generic.GenericRecord;

public abstract class EntitiesRepository {

	protected EntitiesRepository() {
		init();
	}

	abstract protected void init();

	public abstract void saveEntity(GenericRecord record);
	
	public abstract List<Document> queryAllDocuments(double longitude, double latitude);
}
