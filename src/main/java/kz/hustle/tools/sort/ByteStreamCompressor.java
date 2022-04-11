package kz.hustle.tools.sort;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ByteStreamCompressor {

    private static int BUFFER_SIZE = 65536;

    public static byte[] compressSnappy(byte[] bytes) throws IOException {
        return Snappy.compress(bytes);
    }

    public static byte[] uncompressSnappy(byte[] bytes) throws IOException {
        return Snappy.uncompress(bytes);
    }

    public static byte[] compressGzip(byte[] bytes) throws IOException {
        GzipParameters parameters = new GzipParameters();
        parameters.setCompressionLevel(9);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(bytes.length);
        GzipCompressorOutputStream gzipOutputStream = new GzipCompressorOutputStream(bos, parameters);
        Throwable ex = null;
        try {
            gzipOutputStream.write(bytes);
            gzipOutputStream.finish();
            gzipOutputStream.flush();
            bos.close();
        } catch (IOException e) {
            ex = e;
            throw e;
        } finally {
            if (ex != null) {
                try {
                    gzipOutputStream.close();
                } catch (IOException e) {
                    ex.addSuppressed(e);
                }
            } else {
                gzipOutputStream.close();
            }
        }
        return bos.toByteArray();
    }

    public static byte[] decompressGzip(byte[] bytes) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        GzipCompressorInputStream gzipInputStream = new GzipCompressorInputStream(bis);
        Throwable ex = null;
        try {
            byte[] buffer = new byte[BUFFER_SIZE];
            int readed;
            while((readed = gzipInputStream.read(buffer)) > 0) {
                bos.write(buffer, 0, readed);
            }
            bos.close();
            bis.close();
            return bos.toByteArray();
        } catch (IOException e) {
            ex = e;
            throw e;
        } finally {
            if (ex!=null) {
                try {
                    gzipInputStream.close();
                } catch (Throwable e) {
                    ex.addSuppressed(e);
                }
            } else {
                gzipInputStream.close();
            }
        }
    }

}
