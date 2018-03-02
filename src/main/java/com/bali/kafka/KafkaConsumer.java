package com.bali.kafka;

import com.bali.kafka.util.Log;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class KafkaConsumer {

    private static final Log log = Log.getLogger(KafkaConsumer.class);

    private static final int TIMEOUT_MS = 200;

    private org.apache.kafka.clients.consumer.KafkaConsumer stream;

    private Properties consumerProperties;

    private Set<String> topics;

    public KafkaConsumer(Set<String> topics, Properties consumerProperties) {
        this.topics = topics;
        this.consumerProperties = consumerProperties;
    }

    public void init() {
        this.stream = new org.apache.kafka.clients.consumer.KafkaConsumer(consumerProperties);
        this.stream.subscribe(Lists.newArrayList(topics), new KafkaConsumerRebalanceListener());
    }

    public void reInit() {
        if(null != stream) {
            stream.close();
            stream = null;
        }
        init();
    }

    public Iterator<ConsumerRecord<byte[], byte[]>> iterator() {
        return new KafkaRecordIterator();
    }

    public void shutdown() {
        if(null != stream) {
            stream.close();
            stream = null;
        }
    }

    public void commit() {
        stream.commitSync();
    }

    public class KafkaRecordIterator implements Iterator<ConsumerRecord<byte[], byte[]>> {

        private Queue<ConsumerRecord<byte[], byte[]>> records = new ArrayDeque<>();

        @Override
        public boolean hasNext() {
            if(!records.isEmpty())
                return true;
            while(records.isEmpty()) {
                if(null == stream)
                    return false;
                ConsumerRecords<byte[], byte[]> consumerRecords = stream.poll(TIMEOUT_MS);
                if(consumerRecords.count() == 0)
                    continue;
                consumerRecords.forEach(c -> {
                    records.add(c);
                });
            }
            return true;
        }

        @Override
        public ConsumerRecord<byte[], byte[]> next() {
            return records.remove();
        }
    }

    public static class KafkaConsumerRebalanceListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.warn("Partition ownership revoked for : " + Joiner.on(",").join(partitions.stream().map(t -> t.topic() + ":" + t.partition()).toArray()));
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.warn("Partition ownership gained for : " + Joiner.on(",").join(partitions.stream().map(t -> t.topic() + ":" + t.partition()).toArray()));
        }
    }

}
