package kz.hustle.tools.merge;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

public class TestUtils {
    public static void generateParquetFiles(Configuration conf, String outputPath, int numberOfFiles, int recordsInFile) throws IOException {
        FileSystem fs = DistributedFileSystem.get(conf);
        Schema schema = getSchema();
        for (int i = 0; i < numberOfFiles; i++) {
            try (ParquetWriter<GenericRecord> writer = getParquetWriter(conf, outputPath + "/test-parquet-file-" + i + ".parq")) {
                for (int j = 0; j < recordsInFile; j++) {
                    GenericRecord record = new GenericData.Record(schema);
                    record.put("ID", UUID.randomUUID());
                    record.put("VALUE", j);
                    record.put("TIMESTAMP", (new Date()).getTime());
                    writer.write(record);
                }
            }
        }
    }

    private static Schema getSchema() {

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
