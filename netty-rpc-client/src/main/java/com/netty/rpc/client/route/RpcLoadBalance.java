package com.netty.rpc.client.route;

import com.netty.rpc.protocol.RpcProtocol;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by luxiaoxun on 2020-08-01.
 */
public abstract class RpcLoadBalance {

    /**
     * Service map: group by service name
     *
     * @param service
     * @return
     */
    protected Map<String, List<RpcProtocol>> getServiceMap(Map<String, HashSet<RpcProtocol>> service) {
        if (!service.isEmpty()) {
            return service.entrySet().stream()
                    .map(Map.Entry::getValue)
                    .flatMap(Collection::stream)
                    .collect(Collectors.groupingBy(RpcProtocol::getServiceName));
        }
        return Collections.emptyMap();
    }

    /**
     * Route the connection for service key
     *
     * @param serviceName
     * @param service
     * @return
     * @throws Exception
     */
    public abstract RpcProtocol route(String serviceName, Map<String, HashSet<RpcProtocol>> service) throws Exception;
}
