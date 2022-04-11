package kz.hustle.tools;

import kz.hustle.tools.merge.MergingNotCompletedException;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MultithreadedParquetMerger extends ParquetMergerImpl {

    volatile Set<String> brokenFilesMT = ConcurrentHashMap.newKeySet();
    AtomicInteger chunksCounterMT = new AtomicInteger(0);
    AtomicInteger schemaCounterMT = new AtomicInteger(0);


    @Override
    public void merge() throws IOException, InterruptedException, MergingNotCompletedException {
        super.merge();
    }
}
