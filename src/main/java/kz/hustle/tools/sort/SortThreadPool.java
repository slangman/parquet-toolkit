package kz.hustle.tools.sort;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SortThreadPool {
    private static SortThreadPool instance = null;
    protected ExecutorService executorService = Executors.newFixedThreadPool(1);
    private int poolId = -1;

    public SortThreadPool() {
    }

    public static SortThreadPool get() {
        if (instance == null) {
            synchronized (SortThreadPool.class) {
                if (instance == null) {
                    instance = new SortThreadPool();
                }
            }
        }
        return instance;
    }

    public int etPoolId() {
        return this.poolId;
    }

    public SortThreadPool setPoolId(int poolId) {
        this.poolId = poolId;
        return this;
    }

    public int hashCode() {
        return 4 * this.poolId;
    }

    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (!(obj instanceof SortThreadPool)) {
            return false;
        } else {
            SortThreadPool executorService = (SortThreadPool)obj;
            return executorService.hashCode() == this.hashCode();
        }
    }

    public int getPoolSize() {
        ThreadPoolExecutor pool = (ThreadPoolExecutor) this.executorService;
        return pool.getCorePoolSize();
    }

    public SortThreadPool setSize(int poolSize) {
        ThreadPoolExecutor pool = (ThreadPoolExecutor)this.executorService;
        if (pool.getCorePoolSize() != poolSize) {
            pool.setCorePoolSize(poolSize);
            pool.setMaximumPoolSize(poolSize);
        }
        return this;
    }

    public int getActiveTaskCount() {
        ThreadPoolExecutor pool = (ThreadPoolExecutor)this.executorService;
        return pool.getActiveCount();
    }

    public long getCompletedTaskCount() {
        ThreadPoolExecutor pool = (ThreadPoolExecutor)this.executorService;
        return pool.getCompletedTaskCount();
    }

    public int getQueueTaskCount() {
        ThreadPoolExecutor pool = (ThreadPoolExecutor)this.executorService;
        return pool.getQueue().size();
    }

    public boolean isShutDown() {
        ThreadPoolExecutor pool = (ThreadPoolExecutor)this.executorService;
        return pool.isShutdown();
    }

    public SortThreadPool setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    public SortThreadPool shutDown() throws InterruptedException {
        this.executorService.shutdown();
        return this;
    }

    public SortThreadPool await() throws InterruptedException {
        this.executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        return this;
    }

    public SortThreadPool shutDownAndWait() throws InterruptedException {
        this.shutDown();
        this.await();
        return this;
    }

    public SortThreadPool addToPool(Runnable thread) {
        this.executorService.execute(thread);
        return this;
    }
}
