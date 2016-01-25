package de.tu_berlin.kinesistest;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Manages the consumers used in the ThroughputTest
 *
 */
public class ConsumerManager {

	KafkaConsumer<String, String>[] consumers;

	private Random random;
	
	/** 
	 * create the given amount of consumers
	 * @param server the Kafka server
	 * @param numberOfConsumers the number of consumers to create
	 */
	@SuppressWarnings("unchecked")
	public ConsumerManager(String server, int numberOfConsumers) {
	    random = new Random();
		consumers = new KafkaConsumer[numberOfConsumers];
		String[] topics = MessageCreator.getAllTopics();
		for(int i = 0; i < numberOfConsumers; i++) {
			 Properties props = new Properties();
			 int r = random.nextInt(100000);
			 String consumerGroup = "group" + i + "-" + r;
	         props.put("bootstrap.servers", server);
	         props.put("group.id", consumerGroup);
	         props.put("enable.auto.commit", "true");
	         props.put("auto.commit.interval.ms", "1000");
	         props.put("session.timeout.ms", "30000");
	         props.put("replica.fetch.min.bytes", "50240"); // fetch min 100KB
	         props.put("replica.fetch.wait.max.ms", "500");
	         props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	         props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			
	    	 @SuppressWarnings("resource")
			 KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
	    	 consumers[i] = consumer;
	    	 String topic = topics[i % topics.length];
	         consumer.subscribe(Arrays.asList(topic));
	         System.out.println("Created consumer " + i + ": subscribed to " + topic + ", group " + consumerGroup);
		}
	}
	
	/**
	 * returns all consumers
	 * @return
	 */
	public KafkaConsumer<String, String>[] getConsumers() {
		return consumers;
	}

	/**
	 * closes all consumers
	 */
	public void closeAll() {
		for(@SuppressWarnings("rawtypes") KafkaConsumer c : consumers) {
			c.close();
		}
	}
}
