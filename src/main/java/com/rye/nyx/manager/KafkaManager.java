package com.rye.nyx.manager;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.rye.nyx.manager.util.KafkaClientCreator;

/**
 * 
 * @author Rye
 *
 */

public class KafkaManager {

	/**
	 * 
	 * @param topic
	 * @param message
	 * @Description: producer single thread method
	 */
	public void sendMessage(String topic, String message) {
		Producer<String, String> producer = KafkaClientCreator.createProducer();
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
		try {
			// send message
			RecordMetadata metadata = producer.send(record).get();
			System.out.println(
					"Record sent to partition " + metadata.partition() + " with offset " + metadata.offset());
		} catch (ExecutionException | InterruptedException e) {
			System.out.println("Error in sending record");
			e.printStackTrace();
		}

		producer.close();
	}

	/**
	 * 
	 * @param topic
	 * @param message
	 * @Description: consumer single thread method
	 */
	public void consumerMessage(String topic) {
		Consumer<String, String> consumer = KafkaClientCreator.createConsumer();

		while (true) {
			// subscribe topic and consumer message
			consumer.subscribe(Collections.singletonList(topic));

			ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));

			for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {

				System.out.println("Consumer consume message:" + consumerRecord.value());
			}
		}

	}

	
	
}
