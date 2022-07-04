package kz.hustle.tools.simplesort;

import kz.hustle.tools.common.ParquetThread;
import org.apache.avro.generic.GenericRecord;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.List;

public class SortThread extends ParquetThread implements Runnable {
    protected List<GenericRecord> records;
    protected String field;
    protected int threadNumber;
    private SimpleParquetSorter sorter;


    public SortThread(SimpleParquetSorter sorter, List<GenericRecord> records, String field, int threadNumber) {
        this.sorter = sorter;
        this.records = records;
        this.field = field;
        this.threadNumber = threadNumber;
    }

    protected SortThread() {
    }

    @Override
    public void run() {
        List<GenericRecord> sorted = sortPages(records, field);
        ArrayDeque<GenericRecord> queue = new ArrayDeque<>(sorted);
        records.clear();
        sorter.queueList.add(queue);
        System.out.println("Thread " + threadNumber + ": sorting completed");
    }

    protected List<GenericRecord> sortPages(List<GenericRecord> records, String field) {
        records.sort(Comparator.comparing(r -> r.get(field).toString()));
        return records;
    }
}
