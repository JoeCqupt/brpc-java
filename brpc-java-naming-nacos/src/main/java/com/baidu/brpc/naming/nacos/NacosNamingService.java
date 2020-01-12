package com.baidu.brpc.naming.nacos;

import com.baidu.brpc.client.instance.ServiceInstance;
import com.baidu.brpc.naming.*;

import java.util.List;

public class NacosNamingService implements NamingService {
    private BrpcURL url;

    public NacosNamingService(BrpcURL url) {
        this.url = url;
    }

    @Override
    public List<ServiceInstance> lookup(SubscribeInfo subscribeInfo) {
        return null;
    }

    @Override
    public void subscribe(SubscribeInfo subscribeInfo, NotifyListener listener) {

    }

    @Override
    public void unsubscribe(SubscribeInfo subscribeInfo) {

    }

    @Override
    public void register(RegisterInfo registerInfo) {

    }

    @Override
    public void unregister(RegisterInfo registerInfo) {

    }

    @Override
    public void destroy() {

    }
}
