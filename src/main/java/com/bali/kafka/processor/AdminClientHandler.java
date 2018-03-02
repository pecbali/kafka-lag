package com.bali.kafka.processor;

import com.bali.kafka.options.CommandLine;
import com.bali.kafka.util.Log;
import kafka.admin.AdminClient;
import kafka.common.OffsetAndMetadata;
import kafka.coordinator.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import scala.collection.JavaConverters;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class AdminClientHandler {

    private static final Log LOG = Log.getLogger(AdminClientHandler.class);

    private final Map<PartitionKey, OffsetAndMetadata> offsetMap = new ConcurrentSkipListMap<>();
    private final Map<String, Set<Integer>> topicPartitionMap = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> consumerTopicSetMap = new ConcurrentSkipListMap<>();
    private final Map<PartitionKey, MemberMetadata> membershipMap = new ConcurrentSkipListMap<>();

    private AdminClient adminClient;

    private final CommandLine options;

    public AdminClientHandler(CommandLine options) {
        this.options = options;
    }

    void init() {
        Properties props = new Properties();
        props.put("bootstrap.servers", this.options.brokerList);
        adminClient = AdminClient.create(props);
    }

    public void handle(ConsumerRecord<byte[], byte[]> record) {
        try {
            BaseKey key = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(record.key()));
            if (key instanceof OffsetKey) {
                OffsetKey offsetKey = (OffsetKey) key;
                OffsetAndMetadata value = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(record.value()));
                String topic = offsetKey.key().topicPartition().topic();
                String group = offsetKey.key().group();
                int partition = offsetKey.key().topicPartition().partition();
                this.offsetMap.put(new PartitionKey(group, topic, partition), value);
                Set<Integer> partitionSet = null;
                if(topicPartitionMap.containsKey(topic))
                    partitionSet = topicPartitionMap.get(topic);
                else {
                    partitionSet = new TreeSet<>();
                    topicPartitionMap.put(topic, partitionSet);
                }
                partitionSet.add(partition);
                Set<String> topicSet = null;
                if (consumerTopicSetMap.containsKey(group)) {
                    topicSet = consumerTopicSetMap.get(group);
                } else {
                    topicSet = new TreeSet<>();
                    consumerTopicSetMap.put(group, topicSet);
                }
                topicSet.add(topic);

            } else if (key instanceof GroupMetadataKey) {
                GroupMetadataKey metadataKey = (GroupMetadataKey) key;
                GroupMetadata value = GroupMetadataManager.readGroupMessageValue(metadataKey.key(), ByteBuffer.wrap(record.value()));
                JavaConverters.asJavaCollectionConverter(value.allMemberMetadata()).asJavaCollection().forEach(m -> {
                    ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(m.assignment())).partitions().forEach(t -> {
                        membershipMap.put(new PartitionKey(metadataKey.key(), t.topic(), t.partition()), m);
                    });
                });
            }
        } catch (Exception e ) {
            LOG.warn("Failed to process offset topic message",e);
        }
    }



    private String getOwner(PartitionKey partitionKey) {
        if(!membershipMap.containsKey(partitionKey))
            return "Not Known";
        MemberMetadata metadata = membershipMap.get(partitionKey);
        return metadata.clientId()+":"+metadata.clientHost();
    }
    
    public Set<String> getTopicsInConsumerGroup(String group) {
        return consumerTopicSetMap.get(group);
    }

    public Set<String> getConsumerGroups() {
        return consumerTopicSetMap.keySet();
    }

    public PartitionOffset getOffset(PartitionKey key) {
        OffsetAndMetadata metadata = offsetMap.get(key);
        return new PartitionOffset(key.getGroup(),key.getTopic(),key.getPartition(),metadata.offset(),
                -1, getOwner(key), metadata.commitTimestamp());
    }

    public SortedMap<String, SortedMap<Integer, PartitionOffset>> getGroupInfo( String group) {
        SortedMap<String, SortedMap<Integer, PartitionOffset>> returnMap = new TreeMap<>();
        List<PartitionKey> keys = new ArrayList<>();
        if(consumerTopicSetMap.containsKey(group)) {
            consumerTopicSetMap.forEach((k, v) -> {
                v.forEach( topic -> {
                    topicPartitionMap.get(topic).forEach( partition -> {
                        keys.add(new PartitionKey(group,topic, partition));
                    });
                });
            });
        }

        return returnMap;
    }
}
