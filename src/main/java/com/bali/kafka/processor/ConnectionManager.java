package com.bali.kafka.processor;

import org.I0Itec.zkclient.ZkClient;

public class ConnectionManager {

    private final ZkClient zkClient;

    int zkSessionTimeout = 30*60*1000;
    int zkConnectionTimeout = 30*60*1000;

    public ConnectionManager(String zk) {
        this.zkClient = new ZkClient(zk, zkSessionTimeout, zkConnectionTimeout);
    }

    public ZkClient getZkClient() {
        return zkClient;
    }
}
