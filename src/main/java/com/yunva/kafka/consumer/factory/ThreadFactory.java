package com.yunva.kafka.consumer.factory;


import com.yunva.utill.NamedDaemonThreadFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * User: xiang_xiang
 * Date: 2015-10-14
 * PackageName: com.yunva.kafka.consumer.factory
 * Github: https://github.com/ysm20140508
 */
public class ThreadFactory {
    public static ConcurrentHashMap<String, Thread> thradMap = null;
    public static ExecutorService executorService = null;

    public static ConcurrentHashMap getIntstant() {
        if (thradMap == null) {
            thradMap = new ConcurrentHashMap<String, Thread>();
            return thradMap;
        }
        return thradMap;
    }

    public static ExecutorService getThread() {
        if (executorService == null) {
            executorService = Executors.newCachedThreadPool(new NamedDaemonThreadFactory("kafkaDynamicLoad"));
            return executorService;
        }
        return executorService;
    }
}
