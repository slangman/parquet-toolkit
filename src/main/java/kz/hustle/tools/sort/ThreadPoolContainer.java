package kz.hustle.tools.sort;

//import kz.dmc.packages.threads.pools.DMCThreadsPool;

import java.util.concurrent.ConcurrentHashMap;

public class ThreadPoolContainer {
    private static ThreadPoolContainer instance = null;
    private ConcurrentHashMap <String, SortThreadPool> pools = null;

    private ThreadPoolContainer() {
        this.pools = new ConcurrentHashMap<>();
    }

    public static ThreadPoolContainer get() {
        if (instance == null) {
            synchronized(ThreadPoolContainer.class) {
                if (instance == null) {
                    instance = new ThreadPoolContainer();
                }
            }
        }
        return instance;
    }

    public SortThreadPool getPool(String name) {
        SortThreadPool pool = (SortThreadPool)this.pools.get(name);
        if (pool != null) {
            if (pool.isShutDown()) {
                pool = new SortThreadPool();
                this.pools.put(name, pool);
            }
        } else {
            pool = new SortThreadPool();
            this.pools.put(name, pool);
        }

        return pool;
    }

    public boolean isExists(String name) {
        return this.pools.containsKey(name);
    }
}
