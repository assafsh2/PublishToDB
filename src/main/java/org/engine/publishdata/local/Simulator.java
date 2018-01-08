package org.engine.publishdata.local;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.engine.publishdata.stream.StreamBuilder; 
import org.z.entities.schema.DetectionEvent;

import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;

public class Simulator {

	public static void writeData(SchemaRegistryClient schemaRegistry) {
		ProducerSettings<String, Object> setting = createProducerSettings(schemaRegistry);
		for(int i = 0 ; i < 10; i++) {
			String key =  "source0_id"+i; 
			ProducerRecord<String, Object> record;
			try {
				record = new ProducerRecord<>
				("update",key,getGenericRecordForCreation(key));
				setting.createKafkaProducer().send(record);
			} catch (IOException | RestClientException e) {
				e.printStackTrace();
			} 
		}
	}

	private static ProducerSettings<String, Object> createProducerSettings(SchemaRegistryClient schemaRegistry) {
 
		Map<String,String> map = new ConcurrentHashMap<String, String>();		
		map.put("schema.registry.url", "http://fake-url");
		map.put("max.schemas.per.subject", String.valueOf(Integer.MAX_VALUE)); 
		ActorSystem system = ActorSystem.create();

		return ProducerSettings
				.create(system, new StringSerializer(), new KafkaAvroSerializer(schemaRegistry))
				.withBootstrapServers(System.getenv("KAFKA_ADDRESS"));
	}

	protected static GenericRecord getGenericRecordForCreation(String externalSystemID)
			throws IOException, RestClientException { 
		DetectionEvent detectionEvent = DetectionEvent.newBuilder()
				.setSourceName("source0")
				.setExternalSystemID(externalSystemID)
				.setDataOffset(0L)
				.setMetadata("")
				.setPartition(0)
				.build();

		return detectionEvent;
	}
}
