package com.myorg.kafka.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Chaitanya Jadhav
 *
 */

public class KafkaProducerWithCallbackTest {

	public static Logger logger = LoggerFactory.getLogger(KafkaProducerWithCallbackTest.class);

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		String bootstrapServer = "127.0.0.1:9092";
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Create Safe producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");// default is also 5 in
																							// kafka 2.0 >=1.0

		// increase kafka throughput
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "64000");// 64KB
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");// 20 millisecond

		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		for (int i = 1; i <= 10; i++) {
			String topic = "first_topic_new1";
			String value = "Hello word " + Integer.toString(i);
			String key = "id_" + Integer.toString(i);
			logger.info("keys ", key);
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
			producer.send(record, new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception == null) {
						logger.info(metadata.toString());
					}
				}
			});
		}
		producer.flush();
		producer.close();

	}

}
