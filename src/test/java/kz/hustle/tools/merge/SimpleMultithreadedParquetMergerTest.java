package kz.hustle.tools.merge;

import kz.hustle.tools.common.InputPath;
import kz.hustle.tools.common.InputSource;
import kz.hustle.tools.merge.exception.MergingNotCompletedException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.*;

public class SimpleMultithreadedParquetMergerTest {
    private static HdfsConfiguration conf;
    private static MiniDFSCluster cluster;
    private static SimpleMultithreadedParquetMerger simpleMultithreadedParquetMerger;

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleMultithreadedParquetMerger.class);

    private static final String TEST_INPUT_PATH = "/test/merge/";
    private static final String TEST_OUTPUT_PATH = "/test/result/";
    private static final int THREAD_POOL_SIZE = 4;
    private static final int OUTPUT_ROW_GROUP_SIZE = 128 * 1024 * 1024;
    private static final int OUTPUT_CHUNK_SIZE_MEGABYTES = 128;
    private static final int BAD_BLOCKS_READ_ATTEMPTS = 4;
    private static final int BAD_BLOCK_READ_TIMEOUT = 29999;
    private static final int NUMBER_OF_FILES = 10;
    private static final int RECORDS_IN_FILE = 10;

    @BeforeClass
    public static void beforeClass() {
        LOGGER.info("@BeforeClass");

        conf = new HdfsConfiguration();
        try {
            cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
            cluster.getFileSystem().mkdirs(new Path(TEST_INPUT_PATH), FsPermission.valueOf("-rwxrwxrwx"));
            TestUtils.generateParquetFiles(conf, TEST_INPUT_PATH, NUMBER_OF_FILES, RECORDS_IN_FILE);
            FileStatus[] createdFiles = cluster.getFileSystem().listStatus(new Path(TEST_INPUT_PATH));
            LOGGER.info(createdFiles.length + " parquet files generated.");

        } catch (IOException e) {
            e.printStackTrace();
        }
        InputSource inputSource = new InputPath(TEST_INPUT_PATH);
        simpleMultithreadedParquetMerger = SimpleMultithreadedParquetMerger.builder(conf)
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
        assertEquals("outputPath incorrect", simpleMultithreadedParquetMerger.getOutputPath(), TEST_OUTPUT_PATH);
        assertEquals("compressionCodecName incorrect", simpleMultithreadedParquetMerger.getCompressionCodecName(), CompressionCodecName.GZIP);
        assertEquals("threadPoolSize incorrect", THREAD_POOL_SIZE, simpleMultithreadedParquetMerger.getThreadPoolSize());
        assertEquals("outputRowGroupSize incorrect", OUTPUT_ROW_GROUP_SIZE, simpleMultithreadedParquetMerger.getOutputRowGroupSize());
        assertEquals("outputChunkSizeMegabytes incorrect", OUTPUT_CHUNK_SIZE_MEGABYTES * 1024 * 1024, simpleMultithreadedParquetMerger.getOutputChunkSize());
        assertEquals("badBlockReadAttempts incorrect", BAD_BLOCKS_READ_ATTEMPTS, simpleMultithreadedParquetMerger.getBadBlockReadAttempts());
        assertEquals("badBlockReadTimeout incorrect", BAD_BLOCK_READ_TIMEOUT, simpleMultithreadedParquetMerger.getBadBlockReadTimeout());
    }

    @Test
    public void mergeTest() throws MergingNotCompletedException, IOException, InterruptedException {
        LOGGER.info("Test 2: Merge test");
        simpleMultithreadedParquetMerger.merge();
        FileStatus[] resultFiles = cluster.getFileSystem().listStatus(new Path(TEST_OUTPUT_PATH));
        assertEquals(1, resultFiles.length);
        HadoopInputFile inputFile = HadoopInputFile
                .fromPath(resultFiles[0].getPath(), conf);
        long rowsCount = 0;
        try (ParquetFileReader reader = new ParquetFileReader(inputFile, ParquetReadOptions.builder().build())) {
            for (BlockMetaData block : reader.getFooter().getBlocks()) {
                rowsCount += block.getRowCount();
            }
            assertEquals(NUMBER_OF_FILES * RECORDS_IN_FILE, rowsCount);
        }
    }

    @AfterClass
    public static void afterClass() {
        LOGGER.info("@AfterClass");
        cluster.shutdown();
    }

}
