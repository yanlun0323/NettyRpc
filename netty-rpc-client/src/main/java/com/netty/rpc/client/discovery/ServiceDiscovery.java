package com.netty.rpc.client.discovery;

import com.netty.rpc.client.connect.ConnectionManager;
import com.netty.rpc.config.Constant;
import com.netty.rpc.protocol.RpcProtocol;
import com.netty.rpc.util.StringUtils;
import com.netty.rpc.zookeeper.ChildListener;
import com.netty.rpc.zookeeper.CuratorClient;
import org.apache.commons.beanutils.BeanUtilsBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 服务发现
 *
 * @author luxiaoxun
 */
public class ServiceDiscovery {

    private static final Logger logger = LoggerFactory.getLogger(ServiceDiscovery.class);

    private CuratorClient curatorClient;

    public ServiceDiscovery(String registryAddress) {
        this.curatorClient = new CuratorClient(registryAddress);
        discoveryService();
    }

    private void discoveryService() {
        try {
            // Get init service info
            logger.info("Get init service info");
            getServiceAndUpdateServerWithListener();
        } catch (Exception ex) {
            logger.error("Watch node exception: " + ex.getMessage());
        }
    }

    private void getServiceAndUpdateServer() {
        try {
            this.getServiceAndUpdateServer(curatorClient.getChildren("/"));
        } catch (Exception e) {
            logger.error("Get node exception: ", e);
        }
    }

    private void getServiceAndUpdateServerWithListener() {
        try {
            this.getServiceAndUpdateServerWithListener(curatorClient.getChildren("/"));
        } catch (Exception e) {
            logger.error("Get node exception: ", e);
        }
    }

    private void getServiceAndUpdateServer(List<String> serverNodes) {
        try {
            List<RpcProtocol> dataList = new ArrayList<>();
            for (String node : serverNodes) {
                logger.debug("Service node: " + node);

                String childrenPath = String.format("/%s/%s", node, Constant.ZK_PROVIDERS);
                List<String> children = this.curatorClient.getChildren(childrenPath);

                putData(dataList, children);
            }
            logger.debug("Node data: {}", dataList);
            logger.debug("Service discovery triggered updating connected server node.");
            //Update the service info based on the latest data
            updateConnectedServer(dataList);
        } catch (Exception e) {
            logger.error("Get node exception: ", e);
        }
    }

    private void putData(List<RpcProtocol> dataList, List<String> children)
            throws IllegalAccessException, InvocationTargetException {
        for (String srvQueryString : children) {
            Map properties = StringUtils.parseQueryString(srvQueryString);
            RpcProtocol rpcProtocol = new RpcProtocol();
            BeanUtilsBean.getInstance().populate(rpcProtocol, properties);
            dataList.add(rpcProtocol);
        }
    }


    private void getServiceAndUpdateServerWithListener(List<String> serverNodes) {
        try {
            List<RpcProtocol> dataList = new ArrayList<>();
            for (String node : serverNodes) {
                logger.debug("Service node: " + node);

                String childrenPath = String.format("/%s/%s", node, Constant.ZK_PROVIDERS);
                List<String> children = this.curatorClient.getChildrenUsingWatch(childrenPath, new ChildListener() {
                    @Override
                    public void childChanged(String parentPath, List<String> currentChilds) {
                        for (String child : currentChilds) {
                            logger.warn("==> child changed: " + child);
                        }
                        getServiceAndUpdateServer();
                    }
                });
                putData(dataList, children);
            }
            logger.debug("Node data: {}", dataList);
            logger.debug("Service discovery triggered updating connected server node.");
            //Update the service info based on the latest data
            updateConnectedServer(dataList);
        } catch (Exception e) {
            logger.error("Get node exception: ", e);
        }
    }


    private void updateConnectedServer(List<RpcProtocol> dataList) {
        ConnectionManager.getInstance().updateConnectedServer(dataList);
    }

    public void stop() {
        this.curatorClient.close();
    }
}
