package kz.hustle.tools.sort;

import kz.hustle.tools.TestUtils;
import kz.hustle.tools.simplesort.SimpleParquetSorter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.*;

public class SimpleParquetSorterTest extends SorterTest {

    protected static SimpleParquetSorter sorter;

    private static final String TEST_INPUT_PATH = "/test/sort/input/input.parquet";
    private static final String TEST_OUTPUT_PATH = "/test/sort/output/output.parquet";
    private static final Logger LOGGER = LoggerFactory.getLogger(BigParquetSorterTest.class);

    private static final int OUTPUT_CHUNK_SIZE = 256*1024*1024;
    private static final String FILE_FILTER = ".parq";
    private static final int RECORDS_IN_FILE = 1000000;

    @BeforeClass
    public static void beforeClass() throws IOException {
        createMiniDFSCluster();
        TestUtils.generateLargeParquetFile(conf, TEST_INPUT_PATH, RECORDS_IN_FILE);
    }

    @Test
    public void builderTest() throws IOException {
        LOGGER.info("Test 1: Builder test");
        sorter = SimpleParquetSorter.builder()
                .withConfiguration(conf)
                .withInputPath(new Path(TEST_INPUT_PATH))
                .withOutputPath(new Path(TEST_OUTPUT_PATH))
                .withOutputChunkSize(OUTPUT_CHUNK_SIZE)
                .withFileFilter(FILE_FILTER)
                .build();
        assertEquals("inputPath value incorrect", new Path(TEST_INPUT_PATH), sorter.inputPath);
        assertEquals("outputPath value incorrect", new Path(TEST_OUTPUT_PATH), sorter.outputPath);
        assertEquals("outputChunkSize value incorrect", OUTPUT_CHUNK_SIZE, sorter.outputChunkSize);
        assertEquals("fileFilter value incorrect", FILE_FILTER, sorter.fileFilter);
    }

    @Test
    public void sortTest() throws IOException, InterruptedException {
        sorter = SimpleParquetSorter.builder()
                .withConfiguration(conf)
                .withInputPath(new Path(TEST_INPUT_PATH))
                .withOutputPath(new Path(TEST_OUTPUT_PATH))
                .withOutputChunkSize(OUTPUT_CHUNK_SIZE)
                .withFileFilter(FILE_FILTER)
                .build();
        assertEquals("inputPath value incorrect", new Path(TEST_INPUT_PATH), sorter.inputPath);
        assertEquals("outputPath value incorrect", new Path(TEST_OUTPUT_PATH), sorter.outputPath);
        assertEquals("outputChunkSize value incorrect", OUTPUT_CHUNK_SIZE, sorter.outputChunkSize);
        assertEquals("fileFilter value incorrect", FILE_FILTER, sorter.fileFilter);
        sorter.sort("ID");
        FileStatus[] resultFiles = cluster.getFileSystem().listStatus(new Path(TEST_OUTPUT_PATH));
        assertEquals(1, resultFiles.length);
        long rowsCount = TestUtils.getFileRowsCount(resultFiles[0].getPath(), conf);
        assertEquals(RECORDS_IN_FILE, rowsCount);
        assert(fileSorted());
    }

    @AfterClass
    public static void afterClass() {
        cluster.shutdown();
    }

    private boolean fileSorted() throws IOException {
        ParquetReader<GenericRecord> fileReader = AvroParquetReader
                .<GenericRecord>builder(HadoopInputFile.fromPath(new Path(TEST_OUTPUT_PATH), conf))
                .withConf(conf)
                .build();
        List<String> idList = new ArrayList<>();
        GenericRecord record;
        Schema schema = TestUtils.getSchema();
        while ((record = fileReader.read()) != null) {
            idList.add(record.get("ID").toString());
        }
        return idList.stream().sorted().collect(Collectors.toList()).equals(idList);
    }
}
