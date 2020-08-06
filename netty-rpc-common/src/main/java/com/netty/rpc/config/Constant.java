package com.netty.rpc.config;

/**
 * ZooKeeper constant
 *
 * @author luxiaoxun
 */
public interface Constant {

    int ZK_SESSION_TIMEOUT = 5000;
    int ZK_CONNECTION_TIMEOUT = 5000;

    String ZK_PROVIDERS = "providers";

    String ZK_CONSUMERS = "consumers";

    String ZK_NAMESPACE = "netty-rpc";
}
