package kz.hustle.tools;

import kz.hustle.ParquetFile;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultithreadParquetSplitter {
    private Configuration conf;
    private FileSystem fs;
    private Path inputPath;
    private Path outputPath;
    private String outputDir;
    private String outputFileName;
    private int rowGroupSize = 128 * 1024 * 1024;
    private int outputChunkSize = 128 * 1024 * 1024;
    private int threadPoolSize = 8;
    private CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;
    private Schema schema;
    private long recordsPerThread;
    private boolean removeInputFile;
    private boolean removeInputFolder;

    private MultithreadParquetSplitter() {
    }

    public static MultithreadParquetSplitter.Builder builder(ParquetFile parquetFile) {
        return new MultithreadParquetSplitter().new Builder(parquetFile);
    }

    public class Builder {
        private Builder(ParquetFile parquetFile) {
            MultithreadParquetSplitter.this.inputPath = parquetFile.getPath();
            MultithreadParquetSplitter.this.conf = parquetFile.getConf();
        }

        public MultithreadParquetSplitter.Builder withOutputPath(Path outputPath) {
            MultithreadParquetSplitter.this.outputPath = outputPath;
            return this;
        }

        public MultithreadParquetSplitter.Builder withOutputChunkSize(int outputChunkSize) {
            MultithreadParquetSplitter.this.outputChunkSize = outputChunkSize;
            return this;
        }

        public MultithreadParquetSplitter.Builder withRowGroupSize(int rowGroupSize) {
            MultithreadParquetSplitter.this.rowGroupSize = rowGroupSize;
            return this;
        }

        public MultithreadParquetSplitter.Builder withThreadPoolSize(int threadPoolSize) {
            MultithreadParquetSplitter.this.threadPoolSize = threadPoolSize;
            return this;
        }

        public MultithreadParquetSplitter.Builder withCompressionCodec(CompressionCodecName compressionCodecName) {
            MultithreadParquetSplitter.this.compressionCodecName = compressionCodecName;
            return this;
        }

        public MultithreadParquetSplitter.Builder withRemoveInputFile() {
            MultithreadParquetSplitter.this.removeInputFile = true;
            return this;
        }

        public MultithreadParquetSplitter build() {
            return MultithreadParquetSplitter.this;
        }
    }

    public void split() throws Exception {
        long start = System.currentTimeMillis();
        fs = DistributedFileSystem.get(conf);
        if (fs.getFileStatus(inputPath).getLen() <= outputChunkSize) {
            System.out.println("File " + inputPath + " too small to split");
            return;
        }
        if (outputPath == null) {
            setOutputPath();
        } else {
            outputDir = outputPath.toString().replace(conf.get("fs.defaultFS"), "");
            if (!outputDir.endsWith("/")) {
                outputDir = outputDir + "/";
            }
        }
        setOutputFileName();
        setRecordsPerThread();
        if (splitFile() && removeInputFile) {
            if (fs.delete(inputPath, false)) {
                System.out.println(inputPath.toString() + " removed.");
            }
        }
        long finish = System.currentTimeMillis();
        System.out.println("Processing completed in " + MergeUtils.getWorkTime(finish - start));
    }

    private boolean splitFile() {
        try {
            ParquetReader<GenericRecord> reader = createParquetReader();
            ThreadPool pool = new ThreadPool(threadPoolSize);
            GenericRecord record = reader.read();
            int partIndex = 1;

            List<GenericRecord> records = new ArrayList<>();
            while (record != null) {
                if (records.size() == recordsPerThread) {
                    String outputFile = outputDir + outputFileName + "-part" + partIndex + ".parq";
                    ParquetWriter<GenericRecord> writer = createParquetFile(new Path(outputFile));
                    while (pool.getQueueSize() > 1) {
                    }
                    pool.addNewThread(new SplitThread(records, writer, outputFile));
                    //Create new thread;
                    partIndex++;
                    records = new ArrayList<>();
                } else {
                    records.add(record);
                    record = reader.read();
                }
            }
            if (!records.isEmpty()) {
                String outputFile = outputDir + outputFileName + "-part" + partIndex + ".parq";
                ParquetWriter<GenericRecord> writer = createParquetFile(new Path(outputFile));
                pool.addNewThread(new SplitThread(records, writer, outputFile));
            }
            reader.close();
            pool.shutDown();
            pool.await();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    //Вычисляет какое количество записей нужно обрабатывать одним потоком для получения требуемого размера файла на выходе
    private void setRecordsPerThread() throws Exception {
        ParquetReader<GenericRecord> reader = createParquetReader();
        String outputFile = outputDir + outputFileName + "-temp.parq";
        GenericRecord record = reader.read();
        schema = record.getSchema();
        ParquetWriter<GenericRecord> writer = createParquetFile(new Path(outputFile));
        int recordsCount = 1;
        while (record != null && recordsCount <= 50000) {
            writer.write(record);
            record = reader.read();
            recordsCount++;
        }
        reader.close();
        writer.close();
        recordsPerThread = outputChunkSize / (fs.getFileStatus(new Path(outputFile)).getLen() / recordsCount);
        //recordsPerThread = recordsPerThread + recordsPerThread/5;
        System.out.println("Records per thread: " + recordsPerThread);
        fs.delete(new Path(outputFile), true);
    }

    private void setOutputPath() {
        setOutputDir();
        outputPath = new Path(outputDir);
    }

    private void setOutputDir() {
        String inputPathString = inputPath.toString();
        outputDir = inputPathString
                .substring(0, inputPathString.lastIndexOf("/") + 1)
                + "splitted/";
    }

    private void setOutputFileName() {
        String inputPathString = inputPath.toString();
        outputFileName = inputPathString.substring(inputPathString.lastIndexOf("/") + 1, inputPathString.lastIndexOf("."));
    }

    private ParquetReader<GenericRecord> createParquetReader() throws IOException {
        return AvroParquetReader
                .<GenericRecord>builder(HadoopInputFile.fromPath(inputPath, conf))
                .withConf(conf)
                .build();
    }

    private ParquetWriter<GenericRecord> createParquetFile(Path outputPath) throws IOException {
        return AvroParquetWriter.<GenericRecord>builder(outputPath)
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(compressionCodecName)
                .withRowGroupSize(rowGroupSize)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
    }
}
