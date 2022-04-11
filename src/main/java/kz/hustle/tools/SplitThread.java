package kz.hustle.tools;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.util.List;

public class SplitThread implements Runnable {

    private List<GenericRecord> records;
    private ParquetWriter<GenericRecord> writer;
    String outputFile;

    public SplitThread(List<GenericRecord> records, ParquetWriter<GenericRecord> writer, String outputFile) {
        this.records = records;
        this.writer = writer;
        this.outputFile = outputFile;
    }

    @Override
    public void run() {
        records.forEach(record-> {
            try {
                writer.write(record);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        try {
            System.out.println("File: " + outputFile + " - " + writer.getDataSize() / 1024 / 1024 + "Mb");
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
