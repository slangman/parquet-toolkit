package kz.hustle.tools;

import kz.hustle.tools.sort.ParquetSorter;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public abstract class MultithreadedParquetSorter implements ParquetSorter {
    protected Configuration conf;
    //private FileSystem fs;
    protected Path inputPath;
    protected Path outputPath;
    protected int threadPoolSize = 32;
    protected int outputChunkSize;
    protected int rowGroupSize = 128 * 1024 * 1024;
    protected CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;
    protected Schema schema;

    @Override
    public void sort(String field) throws IOException, InterruptedException {

    }

    protected void setOutputPath() {
        String inputPathString = inputPath.toString();
        String outputPathString = inputPathString
                .substring(0, inputPathString.lastIndexOf("/") + 1)
                + "sorted/"
                + inputPathString.substring(inputPathString.lastIndexOf("/") + 1);
        outputPath = new Path(outputPathString);
    }
}
