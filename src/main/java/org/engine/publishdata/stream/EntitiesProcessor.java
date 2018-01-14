package org.engine.publishdata.stream;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.log4j.Logger;
import org.engine.publishdata.db.EntitiesRepository;
import org.engine.publishdata.db.EntitiesRepositoryRedis;
import org.engine.publishdata.utils.Utils; 

public class EntitiesProcessor implements Processor<Object,Object>{	

	final static private Logger logger = Logger.getLogger(EntitiesProcessor.class);
	static {
		Utils.setDebugLevel(logger);
	}

	@Override
	public void init(ProcessorContext context) { 		
	}

	@Override
	public void process(Object key, Object value) {
		logger.debug("Key: "+(String)key);
		logger.debug("Value: "+(GenericRecord)value); 
		
		EntitiesRepository repository = new EntitiesRepositoryRedis();		
		repository.saveEntity((GenericRecord)value);
	}

	@Override
	public void punctuate(long timestamp) {		
	}

	@Override
	public void close() {		
	}
}
