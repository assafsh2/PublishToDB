package org.engine.publishdata.stream;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig; 
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams; 
import org.apache.kafka.streams.processor.TopologyBuilder; 
import org.engine.publishdata.Main;
import org.z.entities.schema.DetectionEvent;

public class StreamBuilder {
	private SchemaRegistryClient schemaRegistry;

	public StreamBuilder() { 
		setSchemaRegistry();
	}

	public SchemaRegistryClient getSchemaRegistry() {
		return schemaRegistry;
	}
	
	public KafkaStreams getStream() {
		TopologyBuilder builder = new TopologyBuilder();

		builder.addSource("messages-source",getDeserializer(true), // keyDeserializer
				getDeserializer(false), // valDeserializer
				"update")
				.addProcessor("processor",
				() -> new EntitiesProcessor(), "messages-source");

		KafkaStreams kafkaStreams = new KafkaStreams(builder, getProperties());
		return kafkaStreams;

	}

	private Properties getProperties() {

		String kafkaAddress;
		String schemaRegustryUrl;
		if (Main.testing) {
			kafkaAddress = "192.168.0.50:9092";
			schemaRegustryUrl = "http://schema-registry.kafka:8081";
		} else {
			kafkaAddress = System.getenv("KAFKA_ADDRESS");
			schemaRegustryUrl = System.getenv("SCHEMA_REGISTRY_ADDRESS");
		}

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				KafkaAvroSerializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				KafkaAvroDeserializer.class);

		props.put("schema.registry.url", schemaRegustryUrl);
		props.put("group.id", "group1");
		props.put("application.id", "publishToDB");

		return props;
	}

	private KafkaAvroDeserializer getDeserializer(boolean isKey) {

		KafkaAvroDeserializer keyDeserializer = new KafkaAvroDeserializer(
				getSchemaRegistry());
		keyDeserializer.configure(Collections.singletonMap(
				"schema.registry.url", "http://fake-url"), isKey);

		return keyDeserializer;
	}

	private SchemaRegistryClient setSchemaRegistry() {
		String schemaRegustryUrl = System.getenv("SCHEMA_REGISTRY_ADDRESS");
		if (schemaRegustryUrl != null) {

			schemaRegistry = new CachedSchemaRegistryClient(schemaRegustryUrl,
					Integer.parseInt(System.getenv("SCHEMA_REGISTRY_IDENTITY")));			
		} else {
			schemaRegistry = new MockSchemaRegistryClient();
			try {
				schemaRegistry.register("DetectionEvent", DetectionEvent.SCHEMA$);
			} catch (IOException | RestClientException e) {
				e.printStackTrace();
			}  
		} 
		return schemaRegistry;
	}
}
