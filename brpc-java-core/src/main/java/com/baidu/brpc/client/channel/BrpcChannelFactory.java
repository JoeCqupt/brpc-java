package com.baidu.brpc.client.channel;

import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.instance.ServiceInstance;

public class BrpcChannelFactory {
    public static BrpcChannel createChannel(ServiceInstance instance, RpcClient rpcClient) {
        // 去跟channel的链接类型
        ChannelType channelType = rpcClient.getRpcClientOptions().getChannelType();
        // 如果使用了连接池的话
        if (channelType == ChannelType.POOLED_CONNECTION) {
            return new BrpcPooledChannel(instance, rpcClient);
        } else if (channelType == ChannelType.SINGLE_CONNECTION) {
            return new BrpcSingleChannel(instance, rpcClient);
        } else if (channelType == ChannelType.SHORT_CONNECTION) {
            return new BrpcShortChannel(instance, rpcClient);
        } else {
            throw new IllegalArgumentException("channel type is not valid:" + channelType.getName());
        }
    }
}
