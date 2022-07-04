package kz.hustle.tools.sort;

import kz.hustle.tools.TestUtils;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static junit.framework.TestCase.assertEquals;

public class BigParquetSorterTest extends SorterTest {

    protected static BigParquetSorter sorter;

    private static final Logger LOGGER = LoggerFactory.getLogger(BigParquetSorterTest.class);

    private static final String TEST_INPUT_PATH = "/test/sort/input";
    private static final String TEST_OUTPUT_PATH = "/test/sort/output";
    private static final int NUMBER_OF_FILES_PROCESSED_IN_PARALLEL = 2;
    private static final int SUBMIT_THREAD_COUNT = 8;
    private static final int SAVE_THREAD_COUNT = 8;
    private static final long BATCH_SIZE = 10000;
    private static final String FILE_FILTER = ".parquet";

    @BeforeClass
    public static void beforeClass() throws IOException {
        createMiniDFSCluster();
        TestUtils.generateParquetFiles(conf, TEST_INPUT_PATH, 10, 100000);
    }

    @Test
    public void builderTest() throws IOException {
        sorter = BigParquetSorter.builder()
                .withConfiguration(conf)
                .withInputPath(new Path(TEST_INPUT_PATH))
                .withOutputPath(new Path(TEST_OUTPUT_PATH))
                .withNumberOfFilesProcessedInParallel(NUMBER_OF_FILES_PROCESSED_IN_PARALLEL)
                .withSubmitThreadCount(SUBMIT_THREAD_COUNT)
                .withSaveThreadCount(SAVE_THREAD_COUNT)
                .withBatchSize(BATCH_SIZE)
                .withFileFilter(".parquet")
                .build();
        assertEquals("inputPath value incorrect", new Path(TEST_INPUT_PATH), sorter.inputPath);
        assertEquals("numberOfFilesProcessedInParallel value incorrect", NUMBER_OF_FILES_PROCESSED_IN_PARALLEL, sorter.getNumberOfFilesProcessedInParallel());
        assertEquals("submitThreadCount value incorrect", SUBMIT_THREAD_COUNT, sorter.getSubmitThreadCount());
        assertEquals("saveThreadCount value incorrect", SAVE_THREAD_COUNT, sorter.getSaveThreadCount());
        assertEquals("batchSize value incorrect", BATCH_SIZE, sorter.getBatchSize());
    }
    
    //Throws ArrayIndexOutOfBoundsException when unable to find any files. Required to fix this. Switch to more obvious exception.
    @Ignore
    @Test
    public void sortTest() throws Exception {
        sorter = BigParquetSorter.builder()
                .withConfiguration(conf)
                .withInputPath(new Path(TEST_INPUT_PATH))
                .withOutputPath(new Path(TEST_OUTPUT_PATH))
                .withNumberOfFilesProcessedInParallel(NUMBER_OF_FILES_PROCESSED_IN_PARALLEL)
                .withSubmitThreadCount(SUBMIT_THREAD_COUNT)
                .withSaveThreadCount(SAVE_THREAD_COUNT)
                .withBatchSize(BATCH_SIZE)
                .withFileFilter(".parq")
                .build();
        sorter.sort("ID");
    }

    @AfterClass
    public static void afterClass() {
        cluster.shutdown();
    }
}
