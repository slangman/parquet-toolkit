package kz.hustle.tools.split;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public abstract class ParquetSplitterImpl implements ParquetSplitter{
    protected Configuration conf;
    protected FileSystem fs;
    protected Path inputPath;
    protected Path outputPath;
    protected String outputDir;
    protected String outputFileName;
    protected long rowGroupSize = 128 * 1024 * 1024;
    protected long outputChunkSize = 128 * 1024 * 1024;
    protected CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;
    protected Schema schema;
    protected int threadPoolSize = 8;

    @Override
    public void split() throws Exception{
    }
}
