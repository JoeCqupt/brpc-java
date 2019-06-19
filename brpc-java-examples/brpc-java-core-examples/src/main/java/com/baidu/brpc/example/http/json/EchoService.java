package com.baidu.brpc.example.http.json;

import com.baidu.brpc.protocol.BrpcMeta;

public interface EchoService {
    @BrpcMeta(serviceName = "HelloWorldService", methodName = "hello")
    String hello(String request);

    @BrpcMeta(serviceName = "HelloWorldService", methodName = "test")
    String test(String param1, String param2);
}
