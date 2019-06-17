/*
 * Copyright (c) 2019 Baidu, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.baidu.brpc.client.loadbalance;

import java.util.List;
import java.util.Set;

import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.channel.BrpcChannel;
import com.baidu.brpc.protocol.Request;

/**
 * load balance strategy interface
 */
public interface LoadBalanceStrategy {
    // default supported load balance type
    // 随机
    int LOAD_BALANCE_RANDOM = 0;
    // 轮询
    int LOAD_BALANCE_ROUND_ROBIN = 1;
    // 权重
    int LOAD_BALANCE_WEIGHT = 2;
    // 根据后端服务器的响应时间判断负载情况，从中选出负载最轻的机器进行分流。这种策略具有很强的自适应性，但是实际的网络环境往往不是那么简单，因此要慎用。
    int LOAD_BALANCE_FAIR = 3;

    void init(RpcClient rpcClient);

    /**
     * select instance channel from total instances
     * @param request request info
     * @param instances total instances, often are all healthy instances
     * @param selectedInstances instances which have been selected.
     * @return the best instance channel
     */
    BrpcChannel selectInstance(
            Request request,
            List<BrpcChannel> instances,
            Set<BrpcChannel> selectedInstances);

    void destroy();
}
