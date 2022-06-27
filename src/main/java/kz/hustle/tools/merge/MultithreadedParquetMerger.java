package kz.hustle.tools.merge;

import kz.hustle.tools.merge.exception.MergingNotCompletedException;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class MultithreadedParquetMerger extends ParquetMergerImpl {

    protected int threadPoolSize = 64;
    protected int badBlockReadAttempts = 5;
    protected long badBlockReadTimeout = 30000;
    protected long outputChunkSize = 128*1024*1024;

    volatile Set<String> brokenFilesMT = ConcurrentHashMap.newKeySet();
    AtomicInteger chunksCounterMT = new AtomicInteger(0);
    AtomicInteger schemaCounterMT = new AtomicInteger(0);

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public long getOutputChunkSize() {
        return outputChunkSize;
    }

    public int getBadBlockReadAttempts() {
        return badBlockReadAttempts;
    }

    public long getBadBlockReadTimeout() {
        return badBlockReadTimeout;
    }

    @Override
    public void merge() throws IOException, InterruptedException, MergingNotCompletedException {
        super.merge();
    }
}
