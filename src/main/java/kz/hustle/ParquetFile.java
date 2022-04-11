package kz.hustle;

import kz.hustle.tools.MergeUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;

public class ParquetFile {
    private Path path;
    private Configuration conf;

    public ParquetFile(Path path, Configuration conf) {
        this.path = path;
        this.conf = conf;
    }

    public Path getPath() {
        return path;
    }

    public Configuration getConf() {
        return conf;
    }

    public long getRowGroupCount() throws IOException {
        ParquetFileReader fileReader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf));
        return fileReader.getRowGroups().size();
    }

    public long getRowsCount() throws IOException {
        ParquetFileReader fileReader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf));
        return fileReader.getRecordCount();
    }

    public void rewrite(Path outputPath) throws IOException {
        long totalRows = getRowsCount();
        System.out.println("Total rows: " + totalRows);
        //long rowsCount = 0;
        System.out.print("Stage 1...");
        long start = System.currentTimeMillis();
        ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(HadoopInputFile.fromPath(path, conf))
                .withConf(conf)
                .build();
        System.out.println("OK");
        System.out.print("Stage 2...");
        GenericRecord record = reader.read();
        System.out.println("OK");
        System.out.print("Stage 3...");
        Schema schema = record.getSchema();
        System.out.println("OK");
        System.out.print("Stage 4...");
        ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputPath)
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.GZIP)
                .withRowGroupSize(128 * 1024 * 1024)//128MB размер блока
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
        while (record != null) {
            //rowsCount++;
            //System.out.println("Writing record " + rowsCount + "/" + totalRows);
            writer.write(record);
            //long recordsSizeMB = (long) writer.getDataSize() / 1024 / 1024;
            //System.out.println(recordsSizeMB);
            record = reader.read();
        }
        writer.close();
        reader.close();
        long finish = System.currentTimeMillis();
        System.out.println("File rewritten in " + MergeUtils.getWorkTime(finish-start));
    }

}
