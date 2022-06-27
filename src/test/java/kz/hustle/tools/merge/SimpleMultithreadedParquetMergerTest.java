package kz.hustle.tools.merge;

import kz.hustle.tools.TestUtils;
import kz.hustle.tools.common.InputPath;
import kz.hustle.tools.common.InputSource;
import kz.hustle.tools.merge.exception.MergingNotCompletedException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.*;

import java.io.IOException;

import static org.junit.Assert.*;

public class SimpleMultithreadedParquetMergerTest extends MergerTest {

    private static SimpleMultithreadedParquetMerger merger;

    private static final int OUTPUT_ROW_GROUP_SIZE = 128 * 1024 * 1024;

    @BeforeClass
    public static void beforeClass() {
        createResources();
        InputSource inputSource = new InputPath(TEST_INPUT_PATH);
        merger = SimpleMultithreadedParquetMerger.builder(conf)
                .inputSource(inputSource)
                .compressionCodec(CompressionCodecName.GZIP)
                .threadPoolSize(THREAD_POOL_SIZE)
                .outputRowGroupSize(OUTPUT_ROW_GROUP_SIZE)
                .outputChunkSizeMegabytes(OUTPUT_CHUNK_SIZE_MEGABYTES)
                .outputPath(TEST_OUTPUT_PATH)
                .removeInputAfterMerging(true, true)
                .badBlockReadAttempts(BAD_BLOCKS_READ_ATTEMPTS)
                .badBlockReadTimeout(BAD_BLOCK_READ_TIMEOUT)
                .withInt96FieldsSupport()
                .build();
    }

    @Test
    public void builderTest() {
        LOGGER.info("Test 1: Builder test");
        assertEquals("outputPath incorrect", TEST_OUTPUT_PATH, merger.getOutputPath());
        assertEquals("compressionCodecName incorrect", CompressionCodecName.GZIP, merger.getCompressionCodecName());
        assertEquals("threadPoolSize incorrect", THREAD_POOL_SIZE, merger.getThreadPoolSize());
        assertEquals("outputRowGroupSize incorrect", OUTPUT_ROW_GROUP_SIZE, merger.getOutputRowGroupSize());
        assertEquals("outputChunkSizeMegabytes incorrect", OUTPUT_CHUNK_SIZE_MEGABYTES * 1024 * 1024, merger.getOutputChunkSize());
        assertEquals("badBlockReadAttempts incorrect", BAD_BLOCKS_READ_ATTEMPTS, merger.getBadBlockReadAttempts());
        assertEquals("badBlockReadTimeout incorrect", BAD_BLOCK_READ_TIMEOUT, merger.getBadBlockReadTimeout());
    }

    @Test
    public void mergeTest() throws MergingNotCompletedException, IOException, InterruptedException {
        LOGGER.info("Test 2: Merge test");
        merger.merge();
        FileStatus[] resultFiles = cluster.getFileSystem().listStatus(new Path(TEST_OUTPUT_PATH));
        assertEquals(1, resultFiles.length);
        long rowsCount = TestUtils.getFileRowsCount(resultFiles[0].getPath(), conf);
        assertEquals(NUMBER_OF_FILES * RECORDS_IN_FILE, rowsCount);
    }

    @AfterClass
    public static void afterClass() {
        LOGGER.info("@AfterClass");
        cluster.shutdown();
    }

}
