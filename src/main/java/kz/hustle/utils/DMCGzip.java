package kz.hustle.utils;


import org.apache.commons.io.IOUtils;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class DMCGzip {

    public static byte[] compress(byte[] decompressedBytes) throws IOException {
//        byte[] compressed = Snappy.compress(decompressedBytes);
//        Deflater compressor = new Deflater();
//        compressor.setLevel(Deflater.BEST_COMPRESSION);
//
//        ByteArrayInputStream bis = new ByteArrayInputStream(decompressedBytes);
        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream(decompressedBytes.length);

//        compressor.setInput(decompressedBytes);
//        compressor.finish();
        GZIPOutputStream gZip = new GZIPOutputStream(byteOutputStream);
        gZip.write(decompressedBytes);
        gZip.close();
        byteOutputStream.close();

//        byte[] buf = new byte[4096];
//        while (!compressor.finished()) {
//            int count = compressor.deflate(buf);
//            byteOutputStream.write(buf, 0, count);
//        }


        return byteOutputStream.toByteArray();
    }

    public byte[] compressSnappy(byte[] decompressedBytes) throws IOException {
        byte[] compressed = Snappy.compress(decompressedBytes);
        return compressed;
    }

    public byte[] decompressSnappy(byte[] compressedBytes) throws IOException {
        byte[] decompressed = Snappy.uncompress(compressedBytes);
        return decompressed;
    }

    public static byte[] decompress(byte[] compressedBytes) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(compressedBytes);
        GZIPInputStream gis = new GZIPInputStream(bis);
        //gis.close();
        return IOUtils.toByteArray(gis);
    }
}
