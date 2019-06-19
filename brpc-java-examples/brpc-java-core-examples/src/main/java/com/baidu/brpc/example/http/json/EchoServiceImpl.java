package com.baidu.brpc.example.http.json;

public class EchoServiceImpl implements EchoService {

    @Override
    public String hello(String request) {
        return "hello " + request;
    }

    @Override
    public String test(String param1, String param2) {
        return param1 + " " + param2;
    }
}
