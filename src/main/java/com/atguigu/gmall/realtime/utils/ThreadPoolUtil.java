package com.atguigu.gmall.realtime.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {

    static ThreadPoolExecutor threadPoolExecutor = null;

    private ThreadPoolUtil() {

    }

    public static ThreadPoolExecutor getThreadPool(){
        if(threadPoolExecutor == null){
            synchronized (ThreadPoolUtil.class){
                if(threadPoolExecutor == null){
                    threadPoolExecutor = new ThreadPoolExecutor(
                            8,16,60, TimeUnit.SECONDS,
                            new LinkedBlockingDeque<>()
                    );
                }
            }

        }
        return threadPoolExecutor;
    }
}
