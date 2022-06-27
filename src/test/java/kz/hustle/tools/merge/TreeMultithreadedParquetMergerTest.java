package kz.hustle.tools.merge;

import kz.hustle.ParquetFolder;
import kz.hustle.tools.TestUtils;
import kz.hustle.tools.common.InputPath;
import kz.hustle.tools.common.InputSource;
import kz.hustle.tools.merge.exception.MergingNotCompletedException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TreeMultithreadedParquetMergerTest extends MergerTest {

    private TreeMultithreadedParquetMerger merger;

    private static final int THREAD_CHUNK_SIZE = 10;
    private static final String OUTPUT_FILE_NAME = "awesome-file-name.parquet";
    private static final long OUTPUT_CHUNK_SIZE = 128 * 1024 * 1024;

    @BeforeClass
    public static void beforeClass() {
        createResources();
    }

    @Test
    public void oldBuilderTest() {
        LOGGER.info("Test 1: Old builder test");
        ParquetFolder folder = new ParquetFolder(new Path(TEST_INPUT_PATH), conf);
        merger = TreeMultithreadedParquetMerger.builder(folder)
                .withThreadPoolSize(THREAD_POOL_SIZE)
                .withThreadChunkSize(THREAD_CHUNK_SIZE)
                .withOutputPath(new Path(TEST_OUTPUT_PATH))
                .withOutputFileName(OUTPUT_FILE_NAME)
                .withOutputChunkSize(OUTPUT_CHUNK_SIZE)
                .withRemoveBrokenFiles(true)
                .withRemoveInputFiles()
                .withRemoveInputDir()
                .build();
        assertEquals("outputPath value incorrect", new Path(TEST_OUTPUT_PATH), merger.getOutputPath());
        assertEquals("compressionCodecName value incorrect", CompressionCodecName.SNAPPY, merger.getCompressionCodecName());
        assertEquals("threadPoolSize value incorrect", THREAD_POOL_SIZE, merger.getThreadPoolSize());
        assertEquals("outputRowGroupSize value incorrect", THREAD_CHUNK_SIZE, merger.getThreadChunkSize());
        assertEquals("outputFileName value incorrect", OUTPUT_FILE_NAME, merger.getOutputFileName());
        assertEquals("outputChunkSize value incorrect", OUTPUT_CHUNK_SIZE, merger.getOutputChunkSize());
        assertEquals("removeBrokenFiles value incorrect", true, merger.isRemoveBrokenFiles());
        assertEquals("removeInputFiles value incorrect", true, merger.isRemoveInputFiles());
        assertEquals("removeInputDir value incorrect", true, merger.isRemoveInputDir());
    }

    @Test
    public void newBuilderTest() {
        LOGGER.info("Test 2: New builder test");
        InputSource inputSource = new InputPath(TEST_INPUT_PATH);
        merger = TreeMultithreadedParquetMerger.builder(conf)
                .inputSource(inputSource)
                .threadPoolSize(THREAD_POOL_SIZE)
                .threadChunkSize(THREAD_CHUNK_SIZE)
                .outputPath(TEST_OUTPUT_PATH)
                .outputChunkSizeMegabytes(OUTPUT_CHUNK_SIZE_MEGABYTES)
                .removeInputAfterMerging(true, true)
                .badBlockReadAttempts(BAD_BLOCKS_READ_ATTEMPTS)
                .badBlockReadTimeout(BAD_BLOCK_READ_TIMEOUT)
                .build();
        assertEquals("threadPoolSize value incorrect", THREAD_POOL_SIZE, merger.getThreadPoolSize());
        assertEquals("threadChunkSize value incorrect", THREAD_CHUNK_SIZE, merger.getThreadChunkSize());
        assertEquals("outputPath value incorrect", TEST_OUTPUT_PATH, merger.getResultPath());
        assertEquals("outputChunkSizeMegabytes value incorrect", OUTPUT_CHUNK_SIZE_MEGABYTES * 1024L * 1024, merger.getOutputChunkSize());
        assertEquals("removeInputFiles value incorrect", true, merger.isRemoveInput());
        assertEquals("moveToTrash value incorrect", true, merger.isMoveToTrash());
        assertEquals("badBlockReadAttempts value incorrect", BAD_BLOCKS_READ_ATTEMPTS, merger.getBadBlockReadAttempts());
        assertEquals("badBlockReadTImeout value incorrect", BAD_BLOCK_READ_TIMEOUT, merger.getBadBlockReadTimeout());
    }

    @Test
    public void mergeTest() throws IOException, MergingNotCompletedException, InterruptedException {
        LOGGER.info("Test 3: Merger test");
        int NUMBER_OF_FILES = 100;
        TestUtils.generateParquetFiles(conf, TEST_INPUT_PATH, 10, RECORDS_IN_FILE);
        ParquetFolder folder = new ParquetFolder(new Path(TEST_INPUT_PATH), conf);
        merger = TreeMultithreadedParquetMerger.builder(folder)
                .withOutputPath(new Path(TEST_OUTPUT_PATH))
                .build();
        merger.merge();
        FileStatus[] resultFiles = cluster.getFileSystem().listStatus(new Path(TEST_OUTPUT_PATH));
        assertEquals(1, resultFiles.length);
        long rowsCount = TestUtils.getFileRowsCount(resultFiles[0].getPath(), conf);
        assertEquals(10 * RECORDS_IN_FILE, rowsCount);
    }

    @AfterClass
    public static void afterClass() {
        LOGGER.info("@AfterClass");
        cluster.shutdown();
    }
}
