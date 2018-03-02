package com.bali.kafka.options;

import com.google.inject.Inject;
import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import io.airlift.airline.Option;

@Command(name = "kafka-lag", description = "Kafka lag monitor utility")
public class CommandLine {

    @Inject
    public HelpOption helpOption;

    @Option(name = {"-p", "--port"}, description = "web Server port")
    public int applicationPort = 8080;

    @Option(name = {"-n", "--numThreads"}, description = "Number of processor threads")
    public int numThreads = Runtime.getRuntime().availableProcessors();

    //@Option(name = {"-s", "--numStreams"}, description = "Number of consumer streams")
    public int numStreams = 1;

    @Option(name = "--offset-topic", description = "offset topic in kafka")
    public String offsetTopic = "__consumer_offsets";

    @Option(name = "--zk", description = "Zookeeper string", required = true)
    public String zookeeper = "";

    @Option(name = "--from-beginning", description = "Zookeeper string")
    public boolean fromStart = false;

    @Option(name = {"-b", "--brokers"}, description = "Brokers list", required = true)
    public String brokerList = "";
    
}
