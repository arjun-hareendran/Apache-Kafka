package com.kafka.v211.producer;

import java.sql.Timestamp;
import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducer {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub

		Properties prop = new Properties();

		// Refer Apache Kafka documentation for the properties used.

		prop.setProperty("bootstrap.servers", "localhost:9092");
		prop.setProperty("key.serializer", StringSerializer.class.getName());
		prop.setProperty("value.serializer", StringSerializer.class.getName());
		prop.setProperty("acks", "1");
		prop.setProperty("retries", "2");
		prop.setProperty("linger.ms ", "1");

		Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(prop);
		ProducerRecord<String, String> producerRecord ;
		
		int counter=1;
		while(counter <100){
			producerRecord = new ProducerRecord<String, String>("secondtopic", "1",
					"Test Message from java program , The current system timestamp is ==> " + new Timestamp(System.currentTimeMillis()));		
			
			producer.send(producerRecord);
			counter++;
			Thread.sleep(100);
		}
		
		// linger.ms will overrider
		// producer.flush();

		producer.close();
		System.out.println("End of Program");
	}

}
