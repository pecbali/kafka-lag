package com.bali.kafka.processor;

public class PartitionOffset {
    private String group;
    private String topic;
    private int partitionId;
    private long offset;
    private long logSize;
    private String owner;
    private long createdAt;

    public PartitionOffset(String group, String topic, int partitionId, long offset, long logSize, String owner, long createdAt) {
        this.group = group;
        this.topic = topic;
        this.partitionId = partitionId;
        this.offset = offset;
        this.logSize = logSize;
        this.owner = owner;
        this.createdAt = createdAt;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public long getOffset() {
        return offset;
    }

    public long getLogSize() {
        return logSize;
    }

    public String getOwner() {
        return owner;
    }

    public long getCreatedAt() {
        return createdAt;
    }
    
    public String getGroup() {
        return group;
    }

    public long getLag() {
        return logSize - offset;
    }

}
