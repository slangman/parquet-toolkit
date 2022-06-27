package kz.hustle.tools.split;

import kz.hustle.ParquetFile;
import kz.hustle.tools.common.ThreadPool;
import kz.hustle.tools.merge.MergeUtils;
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

public class MultithreadedParquetSplitter extends ParquetSplitterImpl {

    private long recordsPerThread;
    private boolean removeInputFile;
    private boolean removeInputFolder;

    private MultithreadedParquetSplitter() {
    }

    @Deprecated
    public static MultithreadedParquetSplitter.DeprecatedBuilder builder(ParquetFile parquetFile) {
        return new MultithreadedParquetSplitter().new DeprecatedBuilder(parquetFile);
    }

    public static MultithreadedParquetSplitter.Builder builder(Configuration conf) {
        return new MultithreadedParquetSplitter().new Builder(conf);
    }

    public class Builder {
        private Builder(Configuration conf) {
            MultithreadedParquetSplitter.this.conf = conf;
        }

        public MultithreadedParquetSplitter.Builder inputFile(String inputFile) {
            MultithreadedParquetSplitter.this.inputPath = new Path(inputFile);
            return this;
        }

        public MultithreadedParquetSplitter.Builder outputPath(String outputPath) {
            MultithreadedParquetSplitter.this.outputPath = new Path(outputPath);
            return this;
        }

        public MultithreadedParquetSplitter.Builder outputChunkSize(long outputChunkSize) {
            MultithreadedParquetSplitter.this.outputChunkSize = outputChunkSize;
            return this;
        }

        public MultithreadedParquetSplitter.Builder outputRowGroupSize(long outputRowGroupSize) {
            MultithreadedParquetSplitter.this.rowGroupSize = outputRowGroupSize;
            return this;
        }

        public MultithreadedParquetSplitter.Builder threadPoolSize(int threadPoolSize) {
            MultithreadedParquetSplitter.this.threadPoolSize = threadPoolSize;
            return this;
        }

        public MultithreadedParquetSplitter.Builder compressionCodec(CompressionCodecName compressionCodecName) {
            MultithreadedParquetSplitter.this.compressionCodecName = compressionCodecName;
            return this;
        }

        public MultithreadedParquetSplitter.Builder removeInputFile(boolean removeInputFile) {
            MultithreadedParquetSplitter.this.removeInputFile = removeInputFile;
            return this;
        }

        public MultithreadedParquetSplitter build() {
            return MultithreadedParquetSplitter.this;
        }
    }

    @Deprecated
    public class DeprecatedBuilder {
        private DeprecatedBuilder(ParquetFile parquetFile) {
            MultithreadedParquetSplitter.this.inputPath = parquetFile.getPath();
            MultithreadedParquetSplitter.this.conf = parquetFile.getConf();
        }

        public MultithreadedParquetSplitter.DeprecatedBuilder withOutputPath(Path outputPath) {
            MultithreadedParquetSplitter.this.outputPath = outputPath;
            return this;
        }

        public MultithreadedParquetSplitter.DeprecatedBuilder withOutputChunkSize(int outputChunkSize) {
            MultithreadedParquetSplitter.this.outputChunkSize = outputChunkSize;
            return this;
        }

        public MultithreadedParquetSplitter.DeprecatedBuilder withRowGroupSize(int rowGroupSize) {
            MultithreadedParquetSplitter.this.rowGroupSize = rowGroupSize;
            return this;
        }

        public MultithreadedParquetSplitter.DeprecatedBuilder withThreadPoolSize(int threadPoolSize) {
            MultithreadedParquetSplitter.this.threadPoolSize = threadPoolSize;
            return this;
        }

        public MultithreadedParquetSplitter.DeprecatedBuilder withCompressionCodec(CompressionCodecName compressionCodecName) {
            MultithreadedParquetSplitter.this.compressionCodecName = compressionCodecName;
            return this;
        }

        public MultithreadedParquetSplitter.DeprecatedBuilder withRemoveInputFile() {
            MultithreadedParquetSplitter.this.removeInputFile = true;
            return this;
        }

        public MultithreadedParquetSplitter build() {
            return MultithreadedParquetSplitter.this;
        }
    }

    @Override
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
