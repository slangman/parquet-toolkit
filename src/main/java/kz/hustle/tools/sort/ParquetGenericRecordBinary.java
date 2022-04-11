package kz.hustle.tools.sort;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ParquetGenericRecordBinary {

    public static byte[] recordToBytes (GenericRecord record, Schema schema) throws IOException {
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        final Encoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
        final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);//GenericDatumWriter
        writer.write(record, encoder);
        encoder.flush();
        final ByteBuffer buf = ByteBuffer.wrap(stream.toByteArray());
        buf.order(ByteOrder.LITTLE_ENDIAN);
        return buf.array();
    }

    public static GenericRecord byteToRecord(byte[] bytes, Schema schema) throws IOException {
        final Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        final DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);// GenericDatumWriter<GenericRecord>();
        return reader.read(null, decoder);
    }

}
