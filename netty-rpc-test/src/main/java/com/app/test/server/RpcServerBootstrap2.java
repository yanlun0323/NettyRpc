package com.app.test.server;

import com.app.test.service.HelloService;
import com.app.test.service.HelloServiceImpl;
import com.app.test.service.PersonService;
import com.app.test.service.PersonServiceImpl;
import com.netty.rpc.server.RpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcServerBootstrap2 {

    private static final Logger logger = LoggerFactory.getLogger(RpcServerBootstrap2.class);

    public static void main(String[] args) {
        String serverAddress = "127.0.0.1:18867";
        String registryAddress = "172.19.80.13:2181";

        RpcServer rpcServer = new RpcServer(serverAddress, registryAddress);

        HelloService helloService = new HelloServiceImpl();
        PersonService personService = new PersonServiceImpl();
        rpcServer.addService(HelloService.class.getName(), helloService);
        rpcServer.addService(PersonService.class.getName(), personService);
        try {
            rpcServer.start();
        } catch (Exception ex) {
            logger.error("Exception: {}", ex);
        }
    }
}
