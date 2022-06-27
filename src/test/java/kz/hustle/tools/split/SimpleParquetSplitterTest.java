package kz.hustle.tools.split;

import kz.hustle.ParquetFile;
import kz.hustle.tools.TestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class SimpleParquetSplitterTest extends SplitterTest{

    private static SimpleParquetSplitter splitter;

    @BeforeClass
    public static void beforeClass() throws IOException {
        createMiniDFSCluster();
        LOGGER.info("Generating large parquet file");
        TestUtils.generateLargeParquetFile(conf, TEST_INPUT_PATH, RECORDS_IN_FILE);
        LOGGER.info("File " + TEST_INPUT_PATH + " generated. Size: "
                + FileUtils.byteCountToDisplaySize(cluster.getFileSystem().getFileStatus(new Path(TEST_INPUT_PATH)).getLen()));
    }

    @Test
    public void oldBuilderTest() {
        LOGGER.info("Test 1: Old builder test");
        ParquetFile file = new ParquetFile(new Path(TEST_INPUT_PATH), conf);
        splitter = SimpleParquetSplitter.builder(file)
                .withOutputPath(new Path(TEST_OUTPUT_PATH))
                .withCompressionCodec(CompressionCodecName.GZIP)
                .withOutputChunkSize(OUTPUT_CHUNK_SIZE)
                .withRowGroupSize(OUTPUT_ROW_GROUP_SIZE)
                .withThreadPoolSize(THREAD_POOL_SIZE)
                .build();
        assertEquals("inputPath value incorrect", new Path(TEST_INPUT_PATH), splitter.inputPath);
        assertEquals("outputPath value incorrect", new Path(TEST_OUTPUT_PATH), splitter.outputPath);
        assertEquals("compressionCodecName value incorrect", CompressionCodecName.GZIP, splitter.compressionCodecName);
        assertEquals("threadPoolSize value incorrect", THREAD_POOL_SIZE, splitter.threadPoolSize);
        assertEquals("rowGroupSize value incorrect", OUTPUT_ROW_GROUP_SIZE, splitter.rowGroupSize);
        assertEquals("outputChunkSize value incorrect", OUTPUT_CHUNK_SIZE, splitter.outputChunkSize);
    }

    @Test
    public void newBuilderTest() {
        LOGGER.info("Test 2: New builder test");
        splitter = SimpleParquetSplitter.builder(conf)
                .inputFile(TEST_INPUT_PATH)
                .outputPath(TEST_OUTPUT_PATH)
                .compressionCodec(CompressionCodecName.GZIP)
                .outputChunkSize(OUTPUT_CHUNK_SIZE)
                .outputRowGroupSize(OUTPUT_ROW_GROUP_SIZE)
                .threadPoolSize(THREAD_POOL_SIZE)
                .build();
        assertEquals("inputPath value incorrect", new Path(TEST_INPUT_PATH), splitter.inputPath);
        assertEquals("outputPath value incorrect", new Path(TEST_OUTPUT_PATH), splitter.outputPath);
        assertEquals("compressionCodecName value incorrect", CompressionCodecName.GZIP, splitter.compressionCodecName);
        assertEquals("threadPoolSize value incorrect", THREAD_POOL_SIZE, splitter.threadPoolSize);
        assertEquals("rowGroupSize value incorrect", OUTPUT_ROW_GROUP_SIZE, splitter.rowGroupSize);
        assertEquals("outputChunkSize value incorrect", OUTPUT_CHUNK_SIZE, splitter.outputChunkSize);
    }

    @Test
    public void splitterTest() throws Exception {
        LOGGER.info("Test 3: Splitter test");
        ParquetFile file = new ParquetFile(new Path(TEST_INPUT_PATH), conf);
        splitter = SimpleParquetSplitter.builder(file)
                .withOutputPath(new Path(TEST_OUTPUT_PATH))
                .withCompressionCodec(CompressionCodecName.GZIP)
                .withOutputChunkSize(OUTPUT_CHUNK_SIZE)
                .withRowGroupSize(OUTPUT_ROW_GROUP_SIZE)
                .withThreadPoolSize(THREAD_POOL_SIZE)
                .build();
        /*splitter = SimpleParquetSplitter.builder(conf)
                .inputFile(TEST_INPUT_PATH)
                .outputPath(TEST_OUTPUT_PATH)
                .compressionCodec(CompressionCodecName.GZIP)
                .outputChunkSize(OUTPUT_CHUNK_SIZE)
                .outputRowGroupSize(OUTPUT_ROW_GROUP_SIZE)
                .threadPoolSize(THREAD_POOL_SIZE)
                .build();*/
        splitter.split();
        FileStatus[] outputFiles = cluster.getFileSystem().listStatus(new Path(TEST_OUTPUT_PATH));
        assertEquals(33, outputFiles.length);
        long recordsCount = 0L;
        for (FileStatus outputFile : outputFiles) {
            recordsCount += TestUtils.getFileRowsCount(outputFile.getPath(),conf);
        }
        assertEquals(recordsCount, RECORDS_IN_FILE);
    }

    @AfterClass
    public static void afterClass() {
        LOGGER.info("@AfterClass");
        cluster.shutdown();
    }
}
