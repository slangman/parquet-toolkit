package kz.hustle;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Daniil Ivantsov
 */
public class ParquetFolder {

    private Path path;
    private Configuration conf;

    public ParquetFolder(Path path, Configuration conf) {
        this.path = path;
        this.conf = conf;
    }

    public Path getPath() {
        return path;
    }

    public Configuration getConf() {
        return conf;
    }

    public void getSchemaCountInFolder() throws IOException {
        Set<String> result = new HashSet<>();
        List<FileStatus> files = Arrays
                .stream(DistributedFileSystem.get(conf).listStatus(path))
                .filter(FileStatus::isFile)
                .collect(Collectors.toList());
        int[] filesCounter = {0};
        files.forEach(file-> {
            filesCounter[0]++;
            System.out.println("Checking file " + filesCounter[0] + "/" + files.size());
            String fileName = file.getPath().toString();
            if (fileName.endsWith(".parq") || fileName.endsWith(".parquet")) {
                ParquetReader<GenericRecord> fileReader = null;
                try {
                    fileReader = AvroParquetReader
                            .<GenericRecord>builder(HadoopInputFile.fromPath(file.getPath(), conf))
                            .withConf(conf)
                            .build();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                GenericRecord record = null;
                try {
                    record = fileReader.read();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                Schema schema = record.getSchema();
                result.add(schema.toString());
            }
        });
        if (result.size()==1) {
            System.out.println("All files in folder have the same schema");
        } else {
            System.out.println("Folder contains files with " + result.size() + " different schemas");
        }
    }

    /**
     * Merges all files in folder with same schema.
     * The merger parameters are default.
     * Result file name will be 'merged_datafile.parquet'
     *//*
    public void merge() {
        ParquetMerger merger = ParquetMerger.builder(path)
                .withConfiguration(configuration)
                .build();
        //merger.merge(path);
    }

    *//**
     * Merges all files in folder with same schema.
     * The merger parameters are default.
     * @param outputFileName Name of the result file
     *//*
    public void merge(String outputFileName) {
        ParquetMerger merger = ParquetMerger.builder(path)
                .withConfiguration(configuration)
                .withOutputFileName(outputFileName)
                .build();
    }

    *//**
     * Merges all files in folder with same schema.
     * Result file name will be 'merged_datafile.parquet'
     * @param threadPoolSize Number of merger threads working simultaneously
     * @param chunkSize Number of files, processed in one thread
     *//*
    public void merge(int threadPoolSize, int chunkSize) {
        ParquetMerger merger = ParquetMerger.builder(path)
                .withConfiguration(configuration)
                .withThreadPoolSize(threadPoolSize)
                .withThreadChunkSize(chunkSize)
                .build();
    }

    *//**
     * Merges all files in folder with same schema
     * @param outputFileName Name of the result file
     * @param threadPoolSize Number of merger threads working simultaneously
     * @param chunkSize Number of files, processed in one thread
     *//*
    public void merge(int threadPoolSize, int chunkSize, String outputFileName) {
        ParquetMerger merger = ParquetMerger.builder(path)
                .withConfiguration(configuration)
                .withThreadPoolSize(threadPoolSize)
                .withThreadChunkSize(chunkSize)
                .withOutputFileName(outputFileName)
                .build();
    }

    public void merge(Path outputPath) {
        ParquetMerger merger = ParquetMerger.builder(path)
                .withConfiguration(configuration)
                .withOutputPath(outputPath)
                .build();
    }

    public void merge(int threadPoolSize, int chunkSize, Path outputPath) {
        ParquetMerger merger = ParquetMerger.builder(path)
                .withConfiguration(configuration)
                .withThreadPoolSize(threadPoolSize)
                .withThreadChunkSize(chunkSize)
                .withOutputPath(outputPath)
                .build();
    }

    public void merge(ParquetMerger merger) {
        merger.merge();
    }

    private ParquetMerger getMergerWithParams(int threadPoolSize, int chunkSize) {
        return ParquetMerger.builder(path)
                .withConfiguration(configuration)
                .withThreadPoolSize(threadPoolSize)
                .withThreadChunkSize(chunkSize)
                .build();
    }*/
}
