package org.engine.publish.db;

import org.apache.avro.generic.GenericRecord;

public abstract class EntitiesRepository {

	protected EntitiesRepository() {
		init();
	}

	abstract protected void init();

	public abstract void saveEntity(GenericRecord record);
}
