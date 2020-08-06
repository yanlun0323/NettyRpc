package com.netty.rpc.client.connect;

import com.netty.rpc.client.handler.RpcClientHandler;
import com.netty.rpc.client.handler.RpcClientInitializer;
import com.netty.rpc.client.route.RpcLoadBalance;
import com.netty.rpc.client.route.impl.RpcLoadBalanceRoundRobin;
import com.netty.rpc.protocol.RpcProtocol;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * RPC Connection Manager
 * Created by luxiaoxun on 2016-03-16.
 */
@SuppressWarnings("all")
public class ConnectionManager {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);

    private EventLoopGroup eventLoopGroup = new NioEventLoopGroup(4);
    private static final ThreadPoolExecutor THREAD_POOL_EXECUTOR = new ThreadPoolExecutor(4, 8,
            600L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(1000));

    private Map<String, RpcClientHandler> connectedServerNodes = new ConcurrentHashMap<>(128);

    private Map<String, HashSet<RpcProtocol>> SERVER = new ConcurrentHashMap<>(128);

    private ReentrantLock lock = new ReentrantLock();
    private Condition connected = lock.newCondition();

    private long waitTimeout = 5000;

    private RpcLoadBalance loadBalance = new RpcLoadBalanceRoundRobin();
    private volatile boolean isRuning = true;

    private ConnectionManager() {
    }

    private static class SingletonHolder {
        private static final ConnectionManager instance = new ConnectionManager();
    }

    public static ConnectionManager getInstance() {
        return SingletonHolder.instance;
    }

    public void updateConnectedServer(List<RpcProtocol> serviceList) {
        THREAD_POOL_EXECUTOR.submit(new Runnable() {
            @Override
            public void run() {
                doUpdateConnectedServer(serviceList);
            }
        });
    }

    private void doUpdateConnectedServer(List<RpcProtocol> serviceList) {
        if (CollectionUtils.isNotEmpty(serviceList)) {
            //update local serverNodes cache
            HashSet<RpcProtocol> serviceSet = new HashSet<>(serviceList);
            // Add new server info
            for (final RpcProtocol protocol : serviceSet) {
                String srvAddr = buildSrvAddr(protocol);
                if (!SERVER.containsKey(srvAddr)) {
                    connectServerNode(protocol, srvAddr);
                }
                HashSet<RpcProtocol> all = SERVER.getOrDefault(srvAddr, new HashSet<>(64));
                all.add(protocol);
                SERVER.putIfAbsent(srvAddr, all);
            }

            // Close and remove invalid server nodes
            for (String srvAddr : connectedServerNodes.keySet()) {
                if (!serviceSet.contains(srvAddr)) {
                    logger.info("Remove invalid service: " + srvAddr);
                    RpcClientHandler handler = connectedServerNodes.get(srvAddr);
                    if (handler != null) {
                        handler.close();
                    }
                    refreshServerAddress(srvAddr);
                }
            }
        } else {
            // No available service
            logger.error("No available service!");
            for (String srvAddr : connectedServerNodes.keySet()) {
                RpcClientHandler handler = connectedServerNodes.get(srvAddr);
                handler.close();
                refreshServerAddress(srvAddr);
            }
        }
    }

    private void refreshServerAddress(String srvAddr) {
        connectedServerNodes.remove(srvAddr);
        SERVER.remove(srvAddr);
    }

    public static String buildSrvAddr(RpcProtocol rpcProtocol) {
        return String.format("%s:%s", rpcProtocol.getHost(), rpcProtocol.getPort());
    }

    private void connectServerNode(RpcProtocol protocol, String srvAddr) {
        logger.info("New Server: {}", srvAddr);

        String[] address = srvAddr.split(":");
        final InetSocketAddress remotePeer = new InetSocketAddress(address[0], Integer.valueOf(address[1]));

        THREAD_POOL_EXECUTOR.submit(() -> {
            Bootstrap b = new Bootstrap();
            b.group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new RpcClientInitializer());

            ChannelFuture channelFuture = b.connect(remotePeer);
            channelFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(final ChannelFuture channelFuture) throws Exception {
                    if (channelFuture.isSuccess()) {
                        logger.info("Successfully connect to remote server. remote peer = " + remotePeer);
                        RpcClientHandler handler = channelFuture.channel().pipeline().get(RpcClientHandler.class);
                        connectedServerNodes.put(srvAddr, handler);
                        signalAvailableHandler();
                    }
                }
            });
        });
    }

    private void signalAvailableHandler() {
        lock.lock();
        try {
            connected.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private boolean waitingForHandler() throws InterruptedException {
        lock.lock();
        try {
            logger.warn("Waiting for available service");
            return connected.await(this.waitTimeout, TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }
    }

    public RpcClientHandler chooseHandler(String serviceName) throws Exception {
        int size = connectedServerNodes.values().size();
        while (isRuning && size <= 0) {
            try {
                waitingForHandler();
                size = connectedServerNodes.values().size();
            } catch (InterruptedException e) {
                logger.error("Waiting for available service is interrupted!", e);
            }
        }

        RpcProtocol protocol = loadBalance.route(serviceName, SERVER);
        return connectedServerNodes.get(String.format("%s:%s", protocol.getHost(), protocol.getPort()));
    }

    public void stop() {
        isRuning = false;
        for (String srvAddr : connectedServerNodes.keySet()) {
            RpcClientHandler handler = connectedServerNodes.get(srvAddr);
            handler.close();
            refreshServerAddress(srvAddr);
        }
        signalAvailableHandler();
        THREAD_POOL_EXECUTOR.shutdown();
        eventLoopGroup.shutdownGracefully();
    }
}
