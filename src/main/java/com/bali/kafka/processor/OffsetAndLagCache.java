package com.bali.kafka.processor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OffsetAndLagCache {
    private String consumerGroupId;
    private Map<String, PartitionOffset> offsetMap = new ConcurrentHashMap<>();
        
}
