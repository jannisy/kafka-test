package de.tu_berlin.kinesistest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * A consumer thread
 *
 */
public class ConsumerThread extends Thread {

	private int msgCount;
	private long msgTotalSize;
	
	long startingTime;	private int executionTime;

	KafkaConsumer<String, String> consumer;
	long actualExecutionTime;

	/**
	 * creates a new consumer thread  for the given consumer
	 * @param consumer the consumer
	 * @param executionTime the execution time
	 */
	public ConsumerThread(KafkaConsumer<String, String> consumer, int executionTime) {
		super();
		this.consumer = consumer;
		this.executionTime = executionTime;
		msgCount = 0;
	}
	/**
	 * returns the number of messages received after the execution
	 * @return the number of messages received after the execution
	 */
	public int getMsgCount() {
		return msgCount;
	}
	/**
	 * returns the amount of data received after the execution in bytes
	 * @return the amount of data received after the execution in bytes
	 */
	public long getMsgSizeTotal() {
		return msgTotalSize;
	}
	/**
	 * returns the actual execution time of the thread
	 * @return the actual execution time of the thread
	 */
	public long getActualExecutionTime() {
		return actualExecutionTime;
	}
	
    public void run() {

    	startingTime =  System.currentTimeMillis();
    	System.out.println("Starting consumer with exec Time " + executionTime);
    	do {
    		ConsumerRecords<String, String> records = consumer.poll(100);

    		msgCount += records.count();
            for (ConsumerRecord<String, String> record : records){
            	msgTotalSize += record.value().length();
            }
    	} while(getCurrentExecutionTime() < executionTime);

    	actualExecutionTime = getCurrentExecutionTimeMs();

    	System.out.println("Done consuming with actual Time " + actualExecutionTime);
    }
    
    /**
     * returns the current running time in seconds.
     * 
     * @return the current running time in seconds.
     */
    private long getCurrentExecutionTime() {
    	long currentTime =  System.currentTimeMillis();
    	return (currentTime - startingTime) / 1000;
    }
    /**
     * returns the current running time in miliseconds.
     * 
     * @return the current running time in miliseconds.
     */
    private long getCurrentExecutionTimeMs() {
    	long currentTime =  System.currentTimeMillis();
    	return (currentTime - startingTime);
    }
}
