package com.myorg.kafka.producer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Chaitanya Jadhav
 *
 */
public class KafkaConsumerDemo {

	public static Logger logger = LoggerFactory.getLogger(KafkaConsumerDemo.class);

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		String bootstrapServer = "127.0.0.1:9092";
		String groupId = "my-fourth-application";
		String topic = "first_topic_new1";
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");

		// Create Consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Collections.singletonList(topic));

		while (true) {
			ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
			logger.info("Received {} records ", consumerRecords.count());
			for (ConsumerRecord<String, String> record : consumerRecords) {
				logger.info("key {}", record.key());
				logger.info("value  {}", record.value());
				logger.info("partition  {}", record.partition());
				logger.info("offset  {}", record.offset());

			}
			logger.info("committing offset..");
			// commitSync is a blocking method. Calling it will block your thread until it
			// either succeeds or fails.
			consumer.commitSync();

			// commitAsync is a non-blocking method. Calling it will not block your thread.
			// consumer.commitAsync();

		}
		// consumer.close();
	}
}
