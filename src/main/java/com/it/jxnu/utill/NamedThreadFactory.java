package com.it.jxnu.utill;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Administrator on 2015-08-26.
 */
public class NamedThreadFactory implements ThreadFactory {
    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    private final String prefix;

    public NamedThreadFactory(String prefix) {
        this.prefix = prefix;
    }

    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setName(prefix + "-" + COUNTER.incrementAndGet());
        thread.setDaemon(false);
        return thread;
    }
}
