package kz.hustle.tools;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.proto.ProtoParquetWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

public class TestUtils {
    public static void generateParquetFiles(Configuration conf, String outputPath, int numberOfFiles, int recordsInFile) throws IOException {
        FileSystem fs = DistributedFileSystem.get(conf);
        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath), true);
        }
        fs.mkdirs(new Path(outputPath));
        Schema schema = getSchema();
        for (int i = 0; i < numberOfFiles; i++) {
            try (ParquetWriter<GenericRecord> writer = getParquetWriter(conf, outputPath + "/test-parquet-file-" + i + ".parq")) {
                for (int j = 0; j < recordsInFile; j++) {
                    GenericRecord record = generateRecord(schema, j);
                    writer.write(record);
                }
            }
        }
    }

    public static long getFileRowsCount(Path path, Configuration conf) throws IOException {
        HadoopInputFile inputFile = HadoopInputFile
                .fromPath(path, conf);
        long rowsCount = 0;
        try (ParquetFileReader reader = new ParquetFileReader(inputFile, ParquetReadOptions.builder().build())) {
            for (BlockMetaData block : reader.getFooter().getBlocks()) {
                rowsCount += block.getRowCount();
            }
        }
        return rowsCount;
    }

    public static void generateLargeParquetFile(Configuration conf, String outputPath, int recordsInFile) throws IOException {
        FileSystem fs = DistributedFileSystem.get(conf);
        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath), true);
        }
        Schema schema = getSchema();
        try (ParquetWriter<GenericRecord> writer = getParquetWriter(conf, outputPath)) {
            for (int j = 0; j < recordsInFile; j++) {
                GenericRecord record = generateRecord(schema, j);
                writer.write(record);
            }
        }
    }

    public static Schema getSchema() {
        String schema = "{" +
                "\"type\": \"record\"," +
                "\"name\": \"test_schema\"," +
                "\"fields\": [" +
                "{\"name\":\"ID\", \"type\":[\"null\",\"string\"]}," +
                "{\"name\":\"VALUE\", \"type\":[\"null\",\"int\"]}," +
                "{\"name\":\"TIMESTAMP\", \"type\":[\"null\",\"long\"]}" +
                "]" +
                "}";
        return new Schema.Parser().parse(schema);
    }

    private static GenericRecord generateRecord(Schema schema, int value) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("ID", UUID.randomUUID());
        record.put("VALUE", value);
        record.put("TIMESTAMP", (new Date()).getTime());
        return record;
    }

    private static ParquetWriter<GenericRecord> getParquetWriter(Configuration conf, String outputPath) throws IOException {
        Path p = new Path(outputPath);
        return AvroParquetWriter.<GenericRecord>builder(new Path(outputPath))
                .withSchema(getSchema())
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.GZIP)
                .withRowGroupSize(128 * 1024 * 1024L)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
    }


}
