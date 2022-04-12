package kz.hustle.tools;

import kz.hustle.tools.common.ParquetThread;
import kz.hustle.utils.DMCGzip;
import kz.hustle.utils.ParquetGenericRecordConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.util.List;

public class AdvancedSortReduceThread extends ParquetThread {
    private List<byte[]> compressedRecords;
    private Schema schema;
    private ParquetWriter<GenericRecord> writer;

    public AdvancedSortReduceThread(List<byte[]> compressedRecords, Schema schema, ParquetWriter<GenericRecord> writer) {
        this.compressedRecords = compressedRecords;
        this.schema = schema;
        this.writer = writer;
    }

    @Override
    public void run() {
        compressedRecords.forEach(r -> {
            try {
                GenericRecord record = ParquetGenericRecordConverter.byteArrayToRecord(DMCGzip.decompress(r), schema);
                writer.write(record);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        try {
            writer.close();
            System.out.println("Reduce thread finished");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
