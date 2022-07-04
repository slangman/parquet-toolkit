package kz.hustle.tools;

import kz.hustle.tools.common.ThreadPool;
import kz.hustle.tools.merge.MergeUtils;
import kz.hustle.tools.sort.BaseParquetSorter;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.util.*;

public class SimpleParquetSorter extends BaseParquetSorter {

    private boolean testVar;
    volatile List<ArrayDeque<GenericRecord>> queueList = new ArrayList<>();


    public SimpleParquetSorter (Builder builder) throws IOException {
       super(builder);
       this.testVar = builder.testVar;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends BaseParquetSorter.Builder<Builder> {

        private boolean testVar;

        @Override
        public Builder getThis() {
            return this;
        }

        public Builder withTestVar(boolean testVar) {
            this.testVar = testVar;
            return this;
        }

        @Override
        public SimpleParquetSorter build() throws IOException {
            return new SimpleParquetSorter(this);
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
        //результаты сохраняем в сериализованном виде в очередях на диске.
        List<GenericRecord> records = new LinkedList<>();
        ParquetReader<GenericRecord> fileReader = AvroParquetReader
                .<GenericRecord>builder(HadoopInputFile.fromPath(inputPath, conf))
                .withConf(conf)
                .build();
        GenericRecord record = fileReader.read();
        schema = record.getSchema();
        while (record != null) {
            records.add(record);
            record = fileReader.read();
            if (records.size() == 2000000) {
                List<GenericRecord> records1 = new ArrayList<>(records);
                threadPool.addNewThread(new SortThread(this, records1, field, threadsCounter));
                records = new ArrayList<>(0);
                threadsCounter++;
            }
        }
        if (records.size() > 0) {
            List<GenericRecord> records1 = new ArrayList<>(records);
            threadPool.addNewThread(new SortThread(this, records1, field, threadsCounter));
            records = new ArrayList<>(0);
        }
        threadPool.shutDown();
        threadPool.await();
        long finish1 = System.currentTimeMillis();
        System.out.println("Stage 1 completed in " + MergeUtils.getWorkTime(finish1 - start1));
        //Потом вычитываем из очередей в выходной файл
        dequeue(field);
    }

    private void dequeue(String field) {
        long counter = 0;
        Map<GenericRecord, Integer> records = new HashMap<>();
        ParquetWriter<GenericRecord> writer;
        try {
            writer = createParquetFile();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        System.out.println("Stage 2 started");
        long start2 = System.currentTimeMillis();
        //Вытаскиваем по одному элементу из каждой очереди, кладем в таблицу.
        for (int i = 0; i < queueList.size(); i++) {
            records.put(queueList.get(i).poll(), i);
        }
        try {
            //Пока хоть в одной очереди есть элементы
            while (recordsExist()) {
                //Берем первый элемент из таблицы и пишем его в файл
                List<GenericRecord> keys = new ArrayList<>(records.keySet());
                keys.sort(Comparator.comparing(r -> r.get(field).toString()));
                GenericRecord record = keys.get(0);
                keys.clear();
                writer.write(record);
                int queueIndex = records.get(record);
                //для теста
                //counter++;
                //System.out.println("Records written: " + counter);
                records.remove(record);
                //Вытаскиваем следующий элемент из той очереди, из которой был взят предыдущий элемент, записанный в файл.
                if (!queueList.get(queueIndex).isEmpty()) {
                    records.put(queueList.get(queueIndex).poll(), queueIndex);
                }
            }
            writer.close();
            long finish2 = System.currentTimeMillis();
            System.out.println("Stage 2 completed in " + MergeUtils.getWorkTime(finish2 - start2));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private boolean recordsExist() {
        boolean result = false;
        for (ArrayDeque<GenericRecord> queue : queueList) {
            if (queue.size() > 0) {
                result = true;
                break;
            }
        }
        return result;
    }

    private ParquetWriter<GenericRecord> createParquetFile() throws IOException {
        return AvroParquetWriter.<GenericRecord>builder(outputPath)
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(compressionCodecName)
                .withRowGroupSize(rowGroupSize)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
    }

    /*private void setOutputPath() {
        String inputPathString = inputPath.toString();
        String outputPathString = inputPathString
                .substring(0, inputPathString.lastIndexOf("/") + 1)
                + "sorted/"
                + inputPathString.substring(inputPathString.lastIndexOf("/") + 1);
        outputPath = new Path(outputPathString);
    }*/

}
