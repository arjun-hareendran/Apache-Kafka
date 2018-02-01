package com.kafka.v211.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Properties prop  = new Properties();
		
		//Refer Apache Kafka documentation for the properties used.
		
		prop.setProperty("bootstrap.servers", "localhost:9091");
		prop.setProperty("key.serializer",StringSerializer.class.getName());
		prop.setProperty("value.serializer",StringSerializer.class.getName());
		prop.setProperty("acks", "1");
		prop.setProperty("retries", "2");
		prop.setProperty("linger.ms ", "1");
				
		
		Producer<String,String> producer  = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(prop);
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("testtopic", "3", "Test Message");
		producer.send(producerRecord);		

		//linger.ms will overrider
		//producer.flush();
		
		producer.close();
		
		System.out.println("End of Program");
	}

}
