package com.yunva.kafka.consumer.factory;

import sun.org.mozilla.javascript.internal.Synchronizer;

import java.util.concurrent.ConcurrentHashMap;

/**
 * User: xiang_xiang
 * Date: 2015-10-14
 * PackageName: com.yunva.kafka.consumer.factory
 * Github: https://github.com/ysm20140508
 */
public class ThreadFactory {
    public static ConcurrentHashMap<String, Thread> thradMap = null;

    public static ConcurrentHashMap getIntstant() {
        if (thradMap == null) {
            thradMap = new ConcurrentHashMap<String, Thread>();
            return thradMap;
        }
        return thradMap;
    }
}
