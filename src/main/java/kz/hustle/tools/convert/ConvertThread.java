package kz.hustle.tools.convert;

import kz.hustle.tools.common.ParquetThread;
import kz.hustle.utils.WorkTime;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;

public class ConvertThread extends ParquetThread {

    private Queue<CSVRecord> lines;
    private String outputPath;
    private Schema schema;
    private Configuration conf;
    private CompressionCodecName compressionCodecName;
    private Map<String, Schema.Type> fieldTypes;

    public ConvertThread(Queue<CSVRecord> lines, String outputPath, Schema schema, Configuration conf, CompressionCodecName compressionCodecName) {
        this.lines = lines;
        this.outputPath = outputPath;
        this.schema = schema;
        this.conf = conf;
        this.compressionCodecName = compressionCodecName;
    }

    public ConvertThread(Queue<CSVRecord> lines,
                         String outputPath,
                         Schema schema,
                         Configuration conf,
                         CompressionCodecName compressionCodecName,
                         Map<String, Schema.Type> fieldTypes) {
        this.lines = lines;
        this.outputPath = outputPath;
        this.schema = schema;
        this.conf = conf;
        this.compressionCodecName = compressionCodecName;
        this.fieldTypes = fieldTypes;
    }

    @Override
    public void run() {
        WorkTime workTime = new WorkTime();
        workTime.startTime();
        try (ParquetWriter<GenericRecord> parquetWriter = AvroParquetWriter.<GenericRecord>builder(new Path(outputPath))
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(compressionCodecName)
                .withRowGroupSize(128 * 1024 * 1024)
                //.withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();) {
            while (!lines.isEmpty()) {
                CSVRecord line = lines.remove();
                Map<String, String> map = line.toMap();
                GenericRecord record = new GenericData.Record(schema);
                map.forEach((k, v) -> {
                    if (!k.equals("")) {
                        if (v.equals("")) {
                            record.put(k, null);
                        } else {
                            if (fieldTypes != null) {
                                record.put(k, parseValue(v, fieldTypes.get(k)));
                            } else {
                                record.put(k, v);
                            }
                        }
                    }
                });
                parquetWriter.write(record);
            }
            System.out.println(outputPath + " processed in " + workTime.getWorkTimeShort());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Object parseValue(String value, Schema.Type type) {
        switch (type) {
            case INT:
                return Integer.parseInt(value);
            case LONG:
                return Long.parseLong(value);
            case FLOAT:
                return Float.parseFloat(value);
            case DOUBLE:
                return Double.parseDouble(value);
            case BOOLEAN:
                return Boolean.parseBoolean(value);
            case NULL:
                return null;
            default:
                return value;
        }
    }

    /*private ParquetWriter<GenericRecord> getParquetWriter(String outputPath) throws IOException {
        Path p = new Path(outputPath);
        FileSystem fs = p.getFileSystem(conf);
        if (fs instanceof LocalFileSystem) {
            return new ProtoParquetWriter
        }
    }*/

}
