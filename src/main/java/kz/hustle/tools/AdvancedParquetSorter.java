package kz.hustle.tools;

import kz.hustle.tools.common.ThreadPool;
import kz.hustle.tools.merge.MergeUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.util.*;

public class AdvancedParquetSorter extends BaseParquetSorter {
    volatile List<ArrayDeque<SerializedRecord>> queueList = new ArrayList<>();

    public AdvancedParquetSorter(Builder builder) throws IOException {
        super(builder);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends BaseParquetSorter.Builder<Builder> {

        @Override
        public Builder getThis() {
            return this;
        }

        @Override
        public AdvancedParquetSorter build() throws IOException {
            return new AdvancedParquetSorter(this);
        }
    }

    @Override
    public void sort(String field) throws IOException, InterruptedException {
        if (outputPath == null) {
            setOutputPath();
        }
        int threadsCounter = 0;
        ThreadPool threadPool = new ThreadPool(threadPoolSize);
        System.out.println("Stage 1 started");
        long start1 = System.currentTimeMillis();
        //Сперва вычитываем все записи из файла, сортируем небольшие порции в отдельных потоках,
        //результаты сохраняем в сериализованном виде в очередях.
        List<GenericRecord> records = new ArrayList<>();
        ParquetReader<GenericRecord> fileReader = AvroParquetReader
                .<GenericRecord>builder(HadoopInputFile.fromPath(inputPath, conf))
                .withConf(conf)
                .build();
        GenericRecord record = fileReader.read();
        schema = record.getSchema();
        while (record != null) {
            records.add(record);
            record = fileReader.read();
            if (records.size() == 4000000) {
                //List<GenericRecord> records1 = new LinkedList<>(records);
                threadPool.addNewThread(new AdvancedSortThread(this, records, field, threadsCounter));
                records = new ArrayList<>();
                threadsCounter++;
            }
        }
        if (records.size() > 0) {
            threadPool.addNewThread(new AdvancedSortThread(this, records, field, threadsCounter));
            //records.clear();
        }
        threadPool.shutDown();
        threadPool.await();
        long finish1 = System.currentTimeMillis();
        System.out.println("Stage 1 completed in " + MergeUtils.getWorkTime(finish1 - start1));
        //Потом вычитываем из очередей в выходной файл
        dequeue(field);
    }

    /*private void dequeue(String field) {
        Map<String, byte[]> records = new TreeMap<>();

        System.out.println("Stage 2 started");
        long start2 = System.currentTimeMillis();
        long decompressTime = 0;
        long writeTime = 0;
        queueList.forEach(q -> {
            q.forEach(e -> {
                records.put(e.getKey(), e.getValue());
            });
        });
        ParquetWriter<GenericRecord> writer;
        try {
            writer = createParquetFile();
            for (byte[] value : records.values()) {
                long decompressTimeStart = System.currentTimeMillis();
                GenericRecord record = ParquetGenericRecordConverter.byteArrayToRecord(DMCGzip.decompress(value), schema);
                long decompressTimeFinish = System.currentTimeMillis();
                decompressTime += (decompressTimeFinish - decompressTimeStart);
                writer.write(record);
                long writeTimeFinish = System.currentTimeMillis();
                writeTime+=(writeTimeFinish-decompressTimeFinish);
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        long finish2 = System.currentTimeMillis();
        System.out.println("Stage 2 completed in " + MergeUtils.getWorkTime(finish2 - start2));
        System.out.println("Decompress time: " + MergeUtils.getWorkTime(decompressTime));
        System.out.println("Write time: " + MergeUtils.getWorkTime(writeTime));
    }*/


    private void dequeue(String field) throws InterruptedException {
        Map<SerializedRecord, Integer> records = new HashMap<>();
        System.out.println("Stage 2 started");
        long start2 = System.currentTimeMillis();
        ThreadPool threadPool = new ThreadPool(threadPoolSize);
        int threadsCounter = 0;
        List<byte[]> compressedRecords = new ArrayList<>();
        //Вытаскиваем по одному элементу из каждой очереди, кладем в таблицу.
        for (int i = 0; i < queueList.size(); i++) {
            records.put(queueList.get(i).poll(), i);
        }
        List<SerializedRecord> list = new ArrayList<>(records.keySet());
        //Пока хоть в одной очереди есть элементы
        while (recordsExist() || !records.isEmpty()) {
            list.sort(Comparator.comparing(SerializedRecord::getSortableField));
            SerializedRecord firstRecord = list.get(0);
            byte[] compressedRecord = firstRecord.getByteArray();
            compressedRecords.add(compressedRecord);
            //System.out.println("Compressed records size: " + compressedRecords.size());
            if (compressedRecords.size() == 2000000) {
                ParquetWriter<GenericRecord> writer;
                try {
                    writer = createParquetFile(threadsCounter);
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
                while (threadPool.getQueueSize()>1) {
                }
                threadPool.addNewThread(new AdvancedSortReduceThread(compressedRecords, schema, writer));
                threadsCounter++;
                compressedRecords = new ArrayList<>();
            }
            int queueIndex = records.remove(firstRecord);
            list.remove(0);
            //list.clear();
            //для теста
            //counter++;
            //System.out.println("Records written: " + counter);
            //Вытаскиваем следующий элемент из той очереди, из которой был взят предыдущий элемент, записанный в файл.
            if (!queueList.get(queueIndex).isEmpty()) {
                SerializedRecord newRecord = queueList.get(queueIndex).poll();
                records.put(newRecord, queueIndex);
                list.add(newRecord);
            }
        }
        if (!compressedRecords.isEmpty()) {
            ParquetWriter<GenericRecord> writer;
            try {
                writer = createParquetFile(threadsCounter);
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
            threadPool.addNewThread(new AdvancedSortReduceThread(compressedRecords, schema, writer));
        }
        threadPool.shutDown();
        threadPool.await();
        long finish2 = System.currentTimeMillis();
        System.out.println("Stage 2 completed in " + MergeUtils.getWorkTime(finish2 - start2));
    }

    private GenericRecord createRecord(String[] values) {
        GenericRecord record = new GenericData.Record(schema);
        List<Schema.Field> fields = schema.getFields();
        for (int i = 0; i < values.length; i++) {
            record.put(fields.get(i).name(), values[i]);
        }
        return record;
    }

    private boolean recordsExist() {
        boolean result = false;
        for (ArrayDeque<SerializedRecord> queue : queueList) {
            if (queue.size() > 0) {
                result = true;
                break;
            }
        }
        return result;
    }

    private ParquetWriter<GenericRecord> createParquetFile(int threadsCounter) throws IOException {
        return AvroParquetWriter.<GenericRecord>builder(new Path(outputPath.toString() + "-part" + threadsCounter))
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(compressionCodecName)
                .withRowGroupSize(rowGroupSize)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
    }
}
