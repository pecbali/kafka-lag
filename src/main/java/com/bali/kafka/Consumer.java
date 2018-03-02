package com.bali.kafka;

import com.bali.kafka.processor.LagProcessor;
import com.bali.kafka.util.Log;
import com.google.common.collect.Sets;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Iterator;
import java.util.Properties;

public class Consumer implements Runnable {

    private final static Log LOG = Log.getLogger(Consumer.class);

    protected volatile boolean shuttingDown = false;

    private final LagProcessor processor;


    private KafkaConsumer consumer;

    public Consumer(Properties consumerConfig, LagProcessor processor) {
        this.processor = processor;
        this.consumer = new KafkaConsumer(Sets.newHashSet(processor.getOptions().offsetTopic), consumerConfig);
        init();
    }

    public void init() {
        this.consumer.init();
    }

    @Override
    public void run() {
        LOG.info("Starting consumer thread ");
        try {
            Iterator<ConsumerRecord<byte[], byte[]>> iter = consumer.iterator();
            while (!shuttingDown) {
                try {
                    while (!shuttingDown) {
                        if (!iter.hasNext()) {
                            shuttingDown = true;
                            continue;
                        }
                        processor.process(iter.next());
                    }
                } catch(Exception e) {
                    if (e instanceof InterruptedException) {
                        if(shuttingDown) {
                            LOG.warn("Caught interruptedException and interrupting present thread. Consumer will exit normally.");
                            break;
                        }
                    }
                    LOG.warn("Caught Exception. Closing and opening the stream again.", e);
                    consumer.reInit();
                    iter = consumer.iterator();
                }
            }

        } catch(Throwable e) {
            LOG.error("Stream unexpectedly exited.", e);
        } finally {
            LOG.info("Committing consumer offsets.");
            commitOffsets();
            LOG.info("Shutting down consumer connectors.");
            consumer.shutdown();
            LOG.info("Consumer thread stopped");
        }
    }

    public void shutdown() {
        shuttingDown = true;
    }
    
    protected void commitOffsets() {
        consumer.commit();
    }
}
