/*
 * Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
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

package com.baidu.brpc.naming.zookeeper;

import com.baidu.brpc.client.instance.Endpoint;
import com.baidu.brpc.client.instance.ServiceInstance;
import com.baidu.brpc.exceptions.RpcException;
import com.baidu.brpc.naming.BrpcURL;
import com.baidu.brpc.naming.Constants;
import com.baidu.brpc.naming.NamingService;
import com.baidu.brpc.naming.NotifyListener;
import com.baidu.brpc.naming.RegisterInfo;
import com.baidu.brpc.naming.SubscribeInfo;
import com.baidu.brpc.utils.CustomThreadFactory;
import com.baidu.brpc.utils.GsonUtils;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.internal.ConcurrentSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ZookeeperNamingService implements NamingService {
    protected BrpcURL url;
    protected CuratorFramework client;
    private int retryInterval;
    private Timer timer;
    protected ConcurrentSet<RegisterInfo> failedRegisters =
            new ConcurrentSet<RegisterInfo>();
    protected ConcurrentSet<RegisterInfo> failedUnregisters =
            new ConcurrentSet<RegisterInfo>();
    protected ConcurrentMap<SubscribeInfo, NotifyListener> failedSubscribes =
            new ConcurrentHashMap<SubscribeInfo, NotifyListener>();
    protected ConcurrentSet<SubscribeInfo> failedUnsubscribes =
            new ConcurrentSet<SubscribeInfo>();
    protected ConcurrentMap<SubscribeInfo, PathChildrenCache> subscribeCacheMap =
            new ConcurrentHashMap<SubscribeInfo, PathChildrenCache>();

    public ZookeeperNamingService(BrpcURL url) {
        this.url = url;
        // 睡眠 timeout
        int sleepTimeoutMs = url.getIntParameter(
                Constants.SLEEP_TIME_MS, Constants.DEFAULT_SLEEP_TIME_MS);
        // 最大重试次数
        int maxTryTimes = url.getIntParameter(
                Constants.MAX_TRY_TIMES, Constants.DEFAULT_MAX_TRY_TIMES);
        // session 过期日期
        int sessionTimeoutMs = url.getIntParameter(
                Constants.SESSION_TIMEOUT_MS, Constants.DEFAULT_SESSION_TIMEOUT_MS);
        // 连接 timeout
        int connectTimeoutMs = url.getIntParameter(
                Constants.CONNECT_TIMEOUT_MS, Constants.DEFAULT_CONNECT_TIMEOUT_MS);
        // 默认命名空间 : ""
        String namespace = Constants.DEFAULT_PATH;
        if (url.getPath().startsWith("/")) {
            // namespace 取值是 url 的 schema://host:port/namespace?query=value
            namespace = url.getPath().substring(1);
        }
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(sleepTimeoutMs, maxTryTimes);
        client = CuratorFrameworkFactory.builder()
                .connectString(url.getHostPorts())
                .connectionTimeoutMs(connectTimeoutMs)
                .sessionTimeoutMs(sessionTimeoutMs)
                .retryPolicy(retryPolicy)
                .namespace(namespace)
                .build();
        client.start();

        // 重试间隔
        this.retryInterval = url.getIntParameter(Constants.INTERVAL, Constants.DEFAULT_INTERVAL);
        // netty 的超时器
        timer = new HashedWheelTimer(new CustomThreadFactory("zookeeper-retry-timer-thread"));
        timer.newTimeout(
                new TimerTask() {
                    @Override
                    public void run(Timeout timeout) throws Exception {
                        try {
                            for (RegisterInfo registerInfo : failedRegisters) {
                                // 如果有服务端有注册失败的 那么就定时去zookeeper去注册该节点信息
                                register(registerInfo);
                            }
                            for (RegisterInfo registerInfo : failedUnregisters) {
                                // 如果有服务端取消注册失败的 那么就定时去zookeeper去取消注册节点信息
                                unregister(registerInfo);
                            }
                            for (Map.Entry<SubscribeInfo, NotifyListener> entry : failedSubscribes.entrySet()) {
                                // 如果该节点有订阅失败的 那么就定时去zookeeper去订阅
                                subscribe(entry.getKey(), entry.getValue());
                            }
                            for (SubscribeInfo subscribeInfo : failedUnsubscribes) {
                                // 如果该节点有取消订阅失败的 那么就定时去zookeeper去取消订阅
                                unsubscribe(subscribeInfo);
                            }
                        } catch (Exception ex) {
                            log.warn("retry timer exception:", ex);
                        }
                        timer.newTimeout(this, retryInterval, TimeUnit.MILLISECONDS);
                    }
                },
                retryInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public List<ServiceInstance> lookup(SubscribeInfo subscribeInfo) {
        String path = getSubscribePath(subscribeInfo);
        List<ServiceInstance> instances = new ArrayList<ServiceInstance>();
        try {
            List<String> childList = client.getChildren().forPath(path);
            for (String child : childList) {
                String childPath = path + "/" + child;
                try {
                    String childData = new String(client.getData().forPath(childPath));
                    Endpoint endpoint = GsonUtils.fromJson(childData, Endpoint.class);
                    instances.add(new ServiceInstance(endpoint));
                } catch (Exception getDataFailedException) {
                    log.warn("get child data failed, path:{}, ex:", childPath, getDataFailedException);
                }
            }
            log.info("lookup {} instances from {}", instances.size(), url);
        } catch (Exception ex) {
            log.warn("lookup end point list failed from {}, msg={}",
                    url, ex.getMessage());
            if (!subscribeInfo.isIgnoreFailOfNamingService()) {
                throw new RpcException("lookup end point list failed from zookeeper failed", ex);
            }
        }
        return instances;
    }

    @Override
    public void subscribe(SubscribeInfo subscribeInfo, final NotifyListener listener) {
        try {
            String path = getSubscribePath(subscribeInfo);
            PathChildrenCache cache = new PathChildrenCache(client, path, true);
            cache.getListenable().addListener(new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                    ChildData data = event.getData();
                    switch (event.getType()) {
                        case CHILD_ADDED: {
                            ServiceInstance instance = GsonUtils.fromJson(
                                    new String(data.getData()), ServiceInstance.class);
                            listener.notify(Collections.singletonList(instance),
                                    Collections.<ServiceInstance>emptyList());
                            break;
                        }
                        case CHILD_REMOVED: {
                            ServiceInstance instance = GsonUtils.fromJson(
                                    new String(data.getData()), ServiceInstance.class);
                            listener.notify(Collections.<ServiceInstance>emptyList(),
                                    Collections.singletonList(instance));
                            break;
                        }
                        case CHILD_UPDATED:
                            break;
                        default:
                            break;
                    }
                }
            });
            cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            failedSubscribes.remove(subscribeInfo);
            subscribeCacheMap.putIfAbsent(subscribeInfo, cache);
            log.info("subscribe success from {}", url);
        } catch (Exception ex) {
            if (!subscribeInfo.isIgnoreFailOfNamingService()) {
                throw new RpcException("subscribe failed from " + url, ex);
            } else {
                failedSubscribes.putIfAbsent(subscribeInfo, listener);
            }
        }
    }

    @Override
    public void unsubscribe(SubscribeInfo subscribeInfo) {
        PathChildrenCache cache = subscribeCacheMap.get(subscribeInfo);
        try {
            if (cache != null) {
                cache.close();
            }
            log.info("unsubscribe success from {}", url);
        } catch (Exception ex) {
            if (!subscribeInfo.isIgnoreFailOfNamingService()) {
                throw new RpcException("unsubscribe failed from " + url, ex);
            } else {
                failedUnsubscribes.add(subscribeInfo);
                return;
            }
        }
        subscribeCacheMap.remove(subscribeInfo);
    }

    @Override
    public void register(RegisterInfo registerInfo) {
        String parentPath = getParentRegisterPath(registerInfo);
        String path = getRegisterPath(registerInfo);
        String pathData = getRegisterPathData(registerInfo);
        try {
            if (client.checkExists().forPath(parentPath) == null) {
                client.create().withMode(CreateMode.PERSISTENT).forPath(parentPath);
            }
            if (client.checkExists().forPath(path) != null) {
                try {
                    client.delete().forPath(path);
                } catch (Exception deleteException) {
                    log.info("zk delete node failed, ignore");
                }
            }
            client.create().withMode(CreateMode.EPHEMERAL).forPath(path, pathData.getBytes());
            log.info("register success to {}", url);
        } catch (Exception ex) {
            if (!registerInfo.isIgnoreFailOfNamingService()) {
                throw new RpcException("Failed to register to " + url, ex);
            } else {
                // 如果服务端不忽略注册失败的话，那么会把注册失败的信息添加到此SET中
                // 会注册一个Timer去继续注册这个服务类
                failedRegisters.add(registerInfo);
                return;
            }
        }
        // 如果注册成功 就在此set中删除此信息，主要是Timer定时重试的时候，方便在重试成功后移除
        failedRegisters.remove(registerInfo);
    }

    @Override
    public void unregister(RegisterInfo registerInfo) {
        String path = getRegisterPath(registerInfo);
        try {
            client.delete().guaranteed().forPath(path);
            log.info("unregister success to {}", url);
        } catch (Exception ex) {
            if (!registerInfo.isIgnoreFailOfNamingService()) {
                throw new RpcException("Failed to unregister from " + url, ex);
            } else {
                failedUnregisters.add(registerInfo);
            }
        }
    }

    public String getSubscribePath(SubscribeInfo subscribeInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append("/");
        sb.append(subscribeInfo.getGroup()).append(":");
        sb.append(subscribeInfo.getInterfaceName()).append(":");
        sb.append(subscribeInfo.getVersion());
        String path = sb.toString();
        return path;
    }

    public String getParentRegisterPath(RegisterInfo registerInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append("/");
        sb.append(registerInfo.getGroup()).append(":");
        sb.append(registerInfo.getInterfaceName()).append(":");
        sb.append(registerInfo.getVersion());
        String path = sb.toString();
        return path;
    }

    public String getRegisterPath(RegisterInfo registerInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append(getParentRegisterPath(registerInfo));
        sb.append("/");
        sb.append(registerInfo.getHost()).append(":").append(registerInfo.getPort());
        String path = sb.toString();
        return path;
    }

    public String getRegisterPathData(RegisterInfo registerInfo) {
        Endpoint endPoint = new Endpoint(registerInfo.getHost(), registerInfo.getPort());
        return GsonUtils.toJson(endPoint);
    }
}
