package kz.hustle.tools.sort;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class BaseParquetSorter implements ParquetSorter {

    protected Configuration conf;
    protected FileSystem fs;
    protected Path inputPath;
    protected Path outputPath;
    protected int threadPoolSize;
    protected int outputChunkSize;
    protected int rowGroupSize;
    protected CompressionCodecName compressionCodecName;
    protected String fileFilter;
    protected Schema schema;
    protected String sortField;

    //protected BaseParquetSorter() {}

    protected BaseParquetSorter(Builder<?> builder) throws IOException {
        this.inputPath = builder.inputPath;
        this.conf = builder.conf;
        this.fs = DistributedFileSystem.get(conf);
        this.outputPath = builder.outputPath;
        this.threadPoolSize = builder.threadPoolSize;
        this.outputChunkSize = builder.outputChunkSize;
        this.rowGroupSize = builder.rowGroupSize;
        this.compressionCodecName = builder.compressionCodecName;
        this.fileFilter = builder.fileFilter;
        this.sortField = builder.sortField;
    }

    public static Builder builder() {
        return new Builder() {
            @Override
            public Builder getThis() {
                return this;
            }
        };
    }

    public abstract static class Builder<T extends Builder<T>> {

        private Configuration conf;
        private Path inputPath;
        private Path outputPath;
        private int threadPoolSize = 32;
        private int outputChunkSize = 128 * 1024 * 1024;
        private int rowGroupSize = 128 * 1024 * 1024;
        private CompressionCodecName compressionCodecName = CompressionCodecName.GZIP;
        private String fileFilter;
        private String sortField;

        public abstract T getThis();

        public T withConfiguration(Configuration conf) {
            this.conf = conf;
            return this.getThis();
        }

        public T withInputPath(Path inputPath) {
            this.inputPath = inputPath;
            return this.getThis();
        }

        public T withOutputPath(Path outputPath) {
            this.outputPath = outputPath;
            return this.getThis();
        }

        protected T withOutputChunkSize(int outputChunkSize) {
            this.outputChunkSize = outputChunkSize;
            return this.getThis();
        }

        public T withThreadPoolSize(int threadPoolSize) {
            this.threadPoolSize = threadPoolSize;
            return this.getThis();
        }

        protected T withRowGroupSize(int rowGroupSize) {
            this.rowGroupSize = rowGroupSize;
            return this.getThis();
        }

        public T withCompressionCodec(CompressionCodecName compressionCodecName) {
            this.compressionCodecName = compressionCodecName;
            return this.getThis();
        }

        public T withFileFilter(String fileFilter) {
            this.fileFilter = fileFilter;
            return this.getThis();
        }

        public T withSortField(String sortColumn) {
            this.sortField = sortColumn;
            return this.getThis();
        }

        public BaseParquetSorter build() throws IOException {
            return new BaseParquetSorter(this);
        }

    }

    public Schema getSchema() {
        return schema;
    }

    @Override
    public void sort(String field) throws Exception {

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
