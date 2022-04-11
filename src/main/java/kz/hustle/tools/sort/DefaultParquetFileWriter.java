package kz.hustle.tools.sort;

import kz.dmc.packages.filesystems.DMCHadoopFileSystem;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class DefaultParquetFileWriter {
    private final String PARQUET_NAME = "datafile";
    private final String PARQUET_PART = "_part_";
    private final String PARQUET_EXT = ".parq";
    private Configuration configuration = null;
    private FileSystem hadoopFileSystem = null;
    private ParquetWriter<GenericRecord> parquetWriter = null;
    private GenericRecord record = null;
    private Schema schema = null;
    private int blockSize = 0;
    private String saveFile = null;
    private String parquetPath = null;
    private Integer currentFilePart = null;

    public DefaultParquetFileWriter() {
    }

    public DefaultParquetFileWriter setFilePart(int currentFilePart) {
        this.currentFilePart = currentFilePart;
        return this;
    }

    public DefaultParquetFileWriter setHadoopFileSystem(FileSystem hadoopFileSystem) {
        this.hadoopFileSystem = hadoopFileSystem;
        return this;
    }

    public DefaultParquetFileWriter setSavePath(String savePath) {
        this.parquetPath = savePath;
        return this;
    }

    public DefaultParquetFileWriter setSaveFile(String saveFile) {
        this.saveFile = saveFile;
        return this;
    }

    public DefaultParquetFileWriter setSchema(Schema schema) {
        this.schema = schema;
        return this;
    }

    public DefaultParquetFileWriter setBlockSize(int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    public DefaultParquetFileWriter setConfiguration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    public DefaultParquetFileWriter createParquetFile() throws IOException {
        if (this.currentFilePart != null) {
            this.saveFile = this.parquetPath + "datafile" + "_part_" + String.format("%05d", this.currentFilePart) + ".parq";
        } else {
            this.saveFile = this.parquetPath + "datafile" + ".parq";
        }

        this.saveFile = this.saveFile + "_saving...";
        this.parquetWriter = AvroParquetWriter
                .<GenericRecord>builder(new Path(this.saveFile))
                .withSchema(this.schema)
                .withConf(this.configuration)
                .withCompressionCodec(CompressionCodecName.GZIP)
                .withRowGroupSize(this.blockSize * 1024 * 1024)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withPageSize(131072)
                .withValidation(false)
                .withDictionaryEncoding(false)
                .build();
        Integer var10001;
        if (this.currentFilePart != null) {
            Integer var1 = this.currentFilePart;
            Integer var2 = this.currentFilePart = this.currentFilePart + 1;
            var10001 = var1;
        } else {
            var10001 = this.currentFilePart;
        }

        this.currentFilePart = var10001;
        return this;
    }

    public void normalizeFileName() throws IOException {
        Path _from = new Path(this.saveFile);
        Path _to = new Path(this.saveFile.replaceAll("_saving...", ""));
        if (this.hadoopFileSystem.exists(_to)) {
            hadoopFileSystem.delete(_to,false);
            this.hadoopFileSystem.delete(_to, false);
        }

        this.hadoopFileSystem.rename(_from, _to);
    }

    public void closeParquetFile() throws IOException {
        this.parquetWriter.close();
        this.normalizeFileName();
    }

    public void appendRecord(GenericRecord record) throws IOException {
        this.parquetWriter.write(record);
    }
}
