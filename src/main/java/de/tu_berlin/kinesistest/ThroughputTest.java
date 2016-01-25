package de.tu_berlin.kinesistest;
import java.lang.reflect.Array;

import org.apache.kafka.clients.consumer.KafkaConsumer;
/**
 * This sample application performs load testing on a 
 * Kafka cluster.
 * 
 * It performs concurrent PUT and READ requests and measures
 * the amount of data read and written within a configurable time span.
 *
 */
public class ThroughputTest 
{

    public static void main( String[] args )
    {
    	
    	if(args.length != 5) {
            System.out.println( "Please specify 5 arguments: servername executionTimeSeconds producers consumers producerThreads" );
    	}
        
    	// default values
        int executionTimeSeconds = 2;
        int producerCount = 1;
        int consumerCount = 0;
        String kafkaServer = "kafka1:9092";
        int producerThreadCount = 1;
        
        // read arguments
    	String customServer = args[0];
    	if(!customServer.equals("default")) {
    		kafkaServer = customServer;
    	}
    	executionTimeSeconds = Integer.parseInt(args[1]);
    	producerCount = Integer.parseInt(args[2]);
    	consumerCount = Integer.parseInt(args[3]);
    	producerThreadCount = Integer.parseInt(args[4]);
    
        
        ProducerManager prodManager = new ProducerManager(kafkaServer, producerCount);
        ConsumerManager consumerManager = new ConsumerManager(kafkaServer, consumerCount);
        
        MessageCreator msgCreator = new MessageCreator();

        ProducerThread[] producerThreads = createProducerThreads(producerThreadCount, prodManager, msgCreator, consumerCount);
        ConsumerThread[] consumerThreads = createConsumerThreads(consumerCount, consumerManager, consumerCount);
        
        Thread[] allThreads = concatenate((Thread[]) producerThreads, (Thread[]) consumerThreads);
        
        long startTime = System.currentTimeMillis();
        startAndJoinThreads(allThreads);
        
        System.out.println("Flushing...");
        long flushStart = System.currentTimeMillis();
        prodManager.flushAll();
        long endTime = System.currentTimeMillis();
        long globalExecTime = endTime - startTime;
        float flushTime = ((float) (endTime-flushStart)) /1000;
        
        aggregateAndPrintStatistics(producerThreads, consumerThreads, globalExecTime, executionTimeSeconds, flushTime);
        
        System.out.println("Closing...");
        prodManager.closeAll();
        consumerManager.closeAll();
    }
    
    /**
     * starts and joins the given threads.
     * @param allThreads the threads to start and join
     */
	private static void startAndJoinThreads(Thread[] allThreads) { 
    	for(Thread t : allThreads) {
	    	t.start();
	    }
	    int i = 0;
	    for(Thread t : allThreads) {
	    	try {
				t.join();
		        i++;
				
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	    }
	}
	/**
	 * creates the producer threads using the given parameters
	 * @param producerThreadCount the thread count
	 * @param prodManager the producer manager
	 * @param msgCreator the message creator
	 * @param executionTimeSeconds the execution time in seconds
	 * @return the created producer threads
	 */
	private static ProducerThread[] createProducerThreads(int producerThreadCount, ProducerManager prodManager, 
    		MessageCreator msgCreator, int executionTimeSeconds) {
        ProducerThread[] producerThreads = new ProducerThread[producerThreadCount];
        for(int i = 0; i < producerThreadCount; i++) {
        	producerThreads[i] = new ProducerThread(prodManager, msgCreator, executionTimeSeconds);
        }
        return producerThreads;
	}
	/**
	 * creates the consumer threads using the given parameters
	 * @param consumerCount the thread count
	 * @param consumerManager the consumer manager
	 * @param executionTimeSeconds the execution time in seconds
	 * @return the created consumer threads
	 */
	private static ConsumerThread[] createConsumerThreads(int consumerCount, ConsumerManager consumerManager, int executionTimeSeconds) {
		ConsumerThread[] consumerThreads = new ConsumerThread[consumerCount];
	    int k = 0;
	    for(KafkaConsumer<String, String> c : consumerManager.getConsumers()) {
	    	consumerThreads[k] = new ConsumerThread(c, executionTimeSeconds);
	    	k++;
	    }
	    return consumerThreads;
	}
	/**
	 * aggregates the statistics from the given threads and prints
	 * the results to the console.
	 * @param producerThreads the producer threads
	 * @param consumerThreads the consumer threads
	 * @param globalExecTime the execution time including flushing
	 * @param executionTimeSeconds the execution time 
	 * @param flushTime the flush time
	 */
    private static void aggregateAndPrintStatistics(
			ProducerThread[] producerThreads, ConsumerThread[] consumerThreads, long globalExecTime, int executionTimeSeconds, float flushTime) {// Aggregate Statistics
	        int msgSentCount = 0;
	        int msgReadCount = 0;
	        long msgSentSizeTotal = 0;
	        long msgReadSizeTotal = 0;
	        int readRateIndividualAvg = 0;
	        int sentRateIndividualAvg = 0;
	        int sentRateGlobal;
	        int i = 0;
	        for(ProducerThread p : producerThreads) {
	        	msgSentCount += p.getMsgCount();
	        	msgSentSizeTotal += p.getMsgSizeTotal();
	        	long actualExecutionTime = p.getActualExecutionTime();
	        	int sentRateKB =  (int) ((double) p.getMsgSizeTotal() / (actualExecutionTime / 1000) / 1024);
	        	sentRateIndividualAvg += sentRateKB;
	        	i++;
	        	System.out.println("Producer " + i + ": " + p.getMsgCount() + " messages, " 
	        			+ ((float)p.getMsgSizeTotal() / 1024 /1024) + " MB, " 
	        			+ sentRateKB + " KB/s, " + (actualExecutionTime / 1000) + "s");
	        }
	        if(producerThreads.length > 0) {
	        	sentRateIndividualAvg /= producerThreads.length;
	        }
	        sentRateGlobal = (int) ((double)msgSentSizeTotal / (globalExecTime / 1000) / 1024);
	
	        i = 0;
	        for(ConsumerThread c : consumerThreads) {
	        	i++;
	        	msgReadCount += c.getMsgCount();
	        	msgReadSizeTotal += c.getMsgSizeTotal();
	        	long actualExecutionTime = c.getActualExecutionTime();
	        	int readRateKB =  (int) ((double) c.getMsgSizeTotal() / (actualExecutionTime / 1000) / 1024);
	        	readRateIndividualAvg += readRateKB;
	        	System.out.println("Consumer " + i + ": " + c.getMsgCount() + " messages, " 
	        			+ (c.getMsgSizeTotal() / 1024) + " KB, " 
	        			+ readRateKB + " KB/s, " + (actualExecutionTime / 1000) + "s");
	        }
	        if(consumerThreads.length > 0) {
	        	readRateIndividualAvg /= consumerThreads.length;
	        }
	        long msgSentSizeTotalCorrected = (long)(msgSentSizeTotal / (executionTimeSeconds + flushTime) * executionTimeSeconds / 1024/1024);
	    	
	    	System.out.println("Flush Time " + flushTime); 
	    	System.out.println("*********** TOTAL ************ "); 
	    	System.out.println("* Write Count " + msgSentCount); 
	    	System.out.println("* Write Size " + (msgSentSizeTotal / 1024 / 1024) + " MB"); 
	    	System.out.println("* Write Size Corrected Flush " + msgSentSizeTotalCorrected + " MB"); 
	    	System.out.println("* Write Rate (ind) " + (sentRateIndividualAvg) + " KB/s"); 
	    	System.out.println("* Write Rate (glob) " + (sentRateGlobal) + " KB/s"); 
	    	System.out.println("* Read Count " + msgReadCount); 
	    	System.out.println("* Read Size " + (msgReadSizeTotal / 1024 / 1024) + " MB"); 
	    	System.out.println("* Read Rate (ind) " + (readRateIndividualAvg) + " KB/s"); 
			
	}
	
	/**
	 * concatenates two arrays to a new array.
	 * @param a the first array
	 * @param b the second array
	 * @return the concatenated array
	 */
	public static <T> T[] concatenate (T[] a, T[] b) {
	    int aLen = a.length;
	    int bLen = b.length;

	    @SuppressWarnings("unchecked")
	    T[] c = (T[]) Array.newInstance(a.getClass().getComponentType(), aLen+bLen);
	    System.arraycopy(a, 0, c, 0, aLen);
	    System.arraycopy(b, 0, c, aLen, bLen);

	    return c;
	}
}
