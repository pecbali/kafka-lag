package com.bali.kafka.processor;

import com.bali.kafka.options.CommandLine;
import com.bali.kafka.Consumer;
import com.bali.kafka.util.Log;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import kafka.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

public class LagProcessor {

    private final static Log LOG = Log.getLogger(LagProcessor.class);

    private final ListeningExecutorService executorService;
    private List<ListenableFuture<?>> consumerFutures = new ArrayList<>();
    private List<Consumer> consumers = new ArrayList<>();
    private MessageHandler handler;
    private final CommandLine options;

    public LagProcessor(CommandLine options) {
        this.executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(options.numStreams));
        this.options = options;
        this.handler = new MessageHandler(options);
    }
    
    public void process(ConsumerRecord<byte[], byte[]> record) {
        handler.handle(record);
    }

    public void init() {
        startConsumers();
    }

    private  void startConsumers() {
        LOG.info("Initializing consumers");
        IntStream.range(0, options.numStreams).forEach(i -> {
            consumers.add(new Consumer(generateConsumerProperties(), this));
        });
        consumers.forEach( c -> consumerFutures.add(executorService.submit(c)));
    }

    private Properties generateConsumerProperties() {
        Properties props = new Properties();
        props.put("group.id", "KafkaOffsetMonitor-" + System.currentTimeMillis());
        props.put("bootstrap.servers", options.brokerList);
        props.put("exclude.internal.topics", "false");
        props.put("auto.commit.enable", "false");
        props.put("auto.offset.reset", (options.fromStart) ? "earliest" : "latest");
        return props;
    }

    public void join() {
        ListenableFuture<?> singleFuture = Futures.successfulAsList(consumerFutures);
        while(true) {
            try {
                singleFuture.get();
                break;
            } catch (InterruptedException e) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e1) { }
            } catch (ExecutionException e) {
                LOG.error("Execution exception for consumer ", e);
                break;
            }
        }
        LOG.warn("All consumers are stopped.");
        this.executorService.shutdownNow();
    }
    
    public void stop() {
        consumers.forEach( c -> c.shutdown());
    }

    public CommandLine getOptions() {
        return options;
    }
}
