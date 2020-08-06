package com.netty.rpc.client.route.impl;

import com.netty.rpc.client.route.RpcLoadBalance;
import com.netty.rpc.protocol.RpcProtocol;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Random load balance
 * Created by luxiaoxun on 2020-08-01.
 */
public class RpcLoadBalanceRandom extends RpcLoadBalance {
    private Random random = new Random();

    public RpcProtocol doRoute(List<RpcProtocol> addressList) {
        int size = addressList.size();
        // Random
        return addressList.get(random.nextInt(size));
    }

    @Override
    public RpcProtocol route(String serviceName, Map<String, HashSet<RpcProtocol>> service) throws Exception {
        Map<String, List<RpcProtocol>> serviceMap = getServiceMap(service);
        List<RpcProtocol> addressList = serviceMap.get(serviceName);
        if (addressList != null && addressList.size() > 0) {
            return doRoute(addressList);
        } else {
            throw new Exception("Can not find connection for service: " + serviceName);
        }
    }
}
