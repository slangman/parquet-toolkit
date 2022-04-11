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
import org.apache.parquet.io.InputFile;

import java.io.IOException;

public class SimpleParquetSplitter {
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
    private String sortField;

    private SimpleParquetSplitter() {
    }

    public static SimpleParquetSplitter.Builder builder(ParquetFile parquetFile) {
        return new SimpleParquetSplitter().new Builder(parquetFile);
    }

    public class Builder {
        private Builder(ParquetFile parquetFile) {
            SimpleParquetSplitter.this.inputPath = parquetFile.getPath();
            SimpleParquetSplitter.this.conf = parquetFile.getConf();
        }

        public SimpleParquetSplitter.Builder withOutputPath(Path outputPath) {
            SimpleParquetSplitter.this.outputPath = outputPath;
            return this;
        }

        public SimpleParquetSplitter.Builder withOutputChunkSize(int outputChunkSize) {
            SimpleParquetSplitter.this.outputChunkSize = outputChunkSize;
            return this;
        }

        public SimpleParquetSplitter.Builder withRowGroupSize(int rowGroupSize) {
            SimpleParquetSplitter.this.rowGroupSize = rowGroupSize;
            return this;
        }

        public SimpleParquetSplitter.Builder withThreadPoolSize(int threadPoolSize) {
            SimpleParquetSplitter.this.threadPoolSize = threadPoolSize;
            return this;
        }

        public SimpleParquetSplitter.Builder withCompressionCodec(CompressionCodecName compressionCodecName) {
            SimpleParquetSplitter.this.compressionCodecName = compressionCodecName;
            return this;
        }

        public SimpleParquetSplitter.Builder withSorting(String sortField) {
            SimpleParquetSplitter.this.sortField = sortField;
            return this;
        }

        public SimpleParquetSplitter build() {
            return SimpleParquetSplitter.this;
        }
    }

    public void split() throws Exception {
        fs = DistributedFileSystem.get(conf);
        if (fs.getFileStatus(inputPath).getLen() <= outputChunkSize) {
            return;
        }
        long start = System.currentTimeMillis();
        long readTime = 0;
        long writeTime = 0;
        ThreadPool threadPool = null;
        if (sortField != null) {
            threadPool = new ThreadPool(threadPoolSize);
        }
        InputFile inputFile = HadoopInputFile.fromPath(inputPath, conf);
        ParquetReader<GenericRecord> fileReader = AvroParquetReader
                .<GenericRecord>builder(inputFile)
                .withConf(conf)
                .build();
        if (outputPath == null) {
            setOutputPath();
        }
        setOutputFileName();
        GenericRecord record = fileReader.read();
        schema = record.getSchema();
        int partIndex = 1;
        String outputFile = outputDir + outputFileName + "-part" + partIndex + ".parq";
        ParquetWriter<GenericRecord> writer = createParquetFile(new Path(outputFile));
        while (record != null) {
            long writeStart = System.currentTimeMillis();
            writer.write(record);
            long writeFinish = System.currentTimeMillis();
            writeTime += writeFinish - writeStart;
            long recordsSize = writer.getDataSize();
            long readStart = System.currentTimeMillis();
            record = fileReader.read();
            long readFinish = System.currentTimeMillis();
            readTime += readFinish - readStart;

            if (recordsSize >= (outputChunkSize - (1024 * 1024)) && record != null) {
                System.out.println(writer.getDataSize());
                System.out.println("File: " + outputFile + " - " + recordsSize / 1024 / 1024 + "Mb");
                System.out.println("Read time: " + MergeUtils.getWorkTime(readTime));
                System.out.println("Write time: " + MergeUtils.getWorkTime(writeTime));
                readTime = 0;
                writeTime = 0;
                writer.close();
                if (threadPool != null) {
                    while (threadPool.getQueueSize() > 1) {
                    }
                    String finalOutputFile = outputFile;
                    threadPool.addNewThread(new ParquetThread() {
                        @Override
                        public void run() {
                            try {
                                createSorter(finalOutputFile).sort(sortField);
                            } catch (IOException | InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    });
                }
                partIndex++;
                outputFile = outputDir + outputFileName + "-part" + partIndex + ".parq";
                writer = createParquetFile(new Path(outputFile));
            }
        }
        writer.close();
        fileReader.close();
        if (threadPool != null) {
            threadPool.shutDown();
            threadPool.await();
        }
        long finish = System.currentTimeMillis();
        System.out.println("Processing completed in " + MergeUtils.getWorkTime(finish - start));
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

    private ParquetWriter<GenericRecord> createParquetFile(Path outputPath) throws IOException {
        return AvroParquetWriter.<GenericRecord>builder(outputPath)
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(compressionCodecName)
                .withRowGroupSize(rowGroupSize)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
    }

    private SimpleParquetSorter createSorter(String outputFile) {
        ParquetFile file = new ParquetFile(new Path(outputFile), conf);
        //TODO: Потом починить
        /*return SimpleParquetSorter.builder(file)
                .withCompressionCodec(CompressionCodecName.GZIP)
                .build();*/
        return null;
    }
}
