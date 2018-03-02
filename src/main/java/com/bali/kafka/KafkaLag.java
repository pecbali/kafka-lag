package com.bali.kafka;

import com.bali.kafka.options.CommandLine;
import com.bali.kafka.processor.LagProcessor;
import com.bali.kafka.util.Log;
import io.airlift.airline.SingleCommand;

import static spark.Spark.port;
import static spark.Spark.staticFiles;

public class KafkaLag {

    private static final Log LOG = Log.getLogger(KafkaLag.class);

    public static void main(String[] args) {
        CommandLine options = SingleCommand.singleCommand(CommandLine.class).parse(args);
        setUpServer(options);
        LagProcessor processor = new LagProcessor(options);
        processor.init();
        try {
            processor.join();
        } catch ( Throwable th) {
            LOG.error("Exception in process", th);
            processor.stop();
        }
        processor.stop();
    }


    private static void setUpServer(CommandLine options) {
        port(options.applicationPort);
        staticFiles.location("/public");
        staticFiles.expireTime(600L);
        setUpRoutes();
    }

    private static void setUpRoutes() {
        
    }
}
