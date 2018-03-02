package com.bali.kafka.processor;

public class PartitionKey implements Comparable<PartitionKey> {
    private String group;
    private String topic;
    private int partition;

    public PartitionKey(String group, String topic, int partition) {
        this.group = group;
        this.topic = topic;
        this.partition = partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PartitionKey that = (PartitionKey) o;

        if (partition != that.partition) return false;
        if (!group.equals(that.group)) return false;
        return topic.equals(that.topic);
    }

    @Override
    public int hashCode() {
        int result = group.hashCode();
        result = 31 * result + topic.hashCode();
        result = 31 * result + partition;
        return result;
    }

    public String getGroup() {
        return group;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }


    @Override
    public int compareTo(PartitionKey that) {
        if (this.group.compareTo(that.group) < 0) {
            return -1;
        } else if (this.group.compareTo(that.group) > 0) {
            return 1;
        }

        if (this.topic.compareTo(that.topic) < 0) {
            return -1;
        } else if (this.topic.compareTo(that.topic) > 0) {
            return 1;
        }

        if (this.partition < that.partition) {
            return -1;
        } else if (this.partition > that.partition) {
            return 1;
        }
        return 0;
    }
}


