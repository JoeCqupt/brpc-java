package com.baidu.brpc.naming.nacos;

import com.baidu.brpc.naming.BrpcURL;
import com.baidu.brpc.naming.NamingService;
import com.baidu.brpc.naming.NamingServiceFactory;

public class NacosNamingFactory implements NamingServiceFactory {
    @Override
    public String getName() {
        return "nacos";
    }

    @Override
    public NamingService createNamingService(BrpcURL url) {
        return new NacosNamingService(url);
    }
}
