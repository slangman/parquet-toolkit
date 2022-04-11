package kz.hustle.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ParquetGenericRecordConverter {
    public static byte[] recordToByteArray(GenericRecord record, Schema schema) throws IOException {
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        final Encoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
        final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);//GenericDatumWriter

        writer.write(record, encoder);

        encoder.flush();

        final ByteBuffer buf = ByteBuffer.wrap(stream.toByteArray());

        buf.order(ByteOrder.LITTLE_ENDIAN);

        return buf.array();
//        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
//        JsonEncoder enc = EncoderFactory.get().jsonEncoder(schema, stream);
//        final DatumWriter writer = new GenericDatumWriter<GenericRecord>(schema);
//        writer.write(record,enc);
//        enc.flush();
//        return stream.toByteArray();
//        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
//        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
//        dataFileWriter.create(schema, outputStream);
//
//        dataFileWriter.append(record);
//
//        dataFileWriter.flush();
//        dataFileWriter.close();
//
//        return outputStream.toByteArray();


    }

    public static GenericRecord byteArrayToRecord(byte[] bytes, Schema schema) throws IOException {
        final Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        final DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);// GenericDatumWriter<GenericRecord>();
        return reader.read(null, decoder);

//        ByteArrayInputStream bio = new ByteArrayInputStream(bytes);
//        JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, bio);
//        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
//        return reader.read(null, jsonDecoder);


        //return null;
//        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
//        final SeekableByteArrayInput inputStream = new SeekableByteArrayInput(bytes);
//        final DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(inputStream, datumReader);
//
//        return dataFileReader.iterator().next();


    }

    public String recordToJsonString(GenericRecord record, Schema schema) throws IOException {
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        final Encoder encoder = EncoderFactory.get().jsonEncoder(schema,stream);
        final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);//GenericDatumWriter

        writer.write(record, encoder);

        encoder.flush();
        //System.out.println(stream.toString());
        return stream.toString();
    }
}
