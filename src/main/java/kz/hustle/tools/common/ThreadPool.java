package kz.hustle.tools.common;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPool {
    //private static MergeThreadPool instance = null;
    private ExecutorService executorService;
    private ExecutorService executorStatus;
    private ThreadPoolExecutor pool;
    private int tasksCounter = 0;

    public ThreadPool(int poolSize) {
        executorService = Executors.newFixedThreadPool(poolSize);
        executorStatus = Executors.newSingleThreadExecutor();
        pool = (ThreadPoolExecutor) executorService;
        executorStatus.execute(() -> {
            //Оставить для подробного логирования
            //System.out.println("tasks = " + tasksCounter);
            //System.out.println("executorService.isShutdown() = " + executorService.isShutdown());
            while (!executorService.isShutdown()) {
                try {
                    Thread.sleep(10000);
                    tasksCounter = pool.getActiveCount() + pool.getQueue().size();
                    //Оставить для подробного логирования
                    //System.out.println("Задач в очереди " + tasksCounter);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            //Оставить для подробного логирования
            //System.out.println("countTask = " + tasksCounter);
            //System.out.println("executorService.isShutdown() = " + executorService.isShutdown());
            while (tasksCounter > 0) {
                try {
                    Thread.sleep(10000);
                    tasksCounter = pool.getActiveCount() + pool.getQueue().size();
                    //Оставить для подробного логирования
                    //System.out.println("Задач в очереди: " + tasksCounter);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            //Оставить для подробного логирования
            //System.out.println("В пуле больше нет задач");
        });

        executorStatus.shutdown();
    }

    /*public MergeThreadPool get() {
        if (instance == null) {
            synchronized (MergeThreadPool.class) {
                if (instance == null) {
                    instance = new MergeThreadPool();
                }
            }
        }
    }*/

    public int getQueueSize() {
        ThreadPoolExecutor pool = (ThreadPoolExecutor) executorService;
        return pool.getQueue().size();
    }

    public void addNewThread(Runnable thread) {
        executorService.execute(thread);
    }

    public void shutDown() {
        executorService.shutdown();
    }

    public void await() throws InterruptedException {
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }
}
