package kz.hustle.tools;

import kz.hustle.utils.DMCGzip;
import kz.hustle.utils.ParquetGenericRecordConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.*;

public class AdvancedSortThread extends ParquetThread {

    private final Schema schema;
    protected List<GenericRecord> records;
    protected String field;
    protected int threadNumber;
    private AdvancedParquetSorter sorter;

    public AdvancedSortThread(AdvancedParquetSorter sorter, List<GenericRecord> records, String field, int threadNumber) {
        super();
        this.records = records;
        this.field = field;
        this.threadNumber = threadNumber;
        this.sorter = sorter;
        schema = sorter.getSchema();
    }

    @Override
    public void run() {
        List<SerializedRecord> serializedRecords = new LinkedList<>();
        records.forEach(record-> {
            try {
                serializedRecords.add(new SerializedRecord(record.get(field).toString(), DMCGzip.compress(ParquetGenericRecordConverter.recordToByteArray(record, schema))));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        serializedRecords.sort(Comparator.comparing(SerializedRecord::getSortableField));
        ArrayDeque<SerializedRecord> queue = new ArrayDeque<>(serializedRecords);
        sorter.queueList.add(queue);
        records.clear();
        System.out.println("Thread " + threadNumber + ": sorting completed");
    }
}
