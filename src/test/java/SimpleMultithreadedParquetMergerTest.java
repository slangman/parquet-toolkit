import kz.hustle.ParquetFolder;
import kz.hustle.tools.merge.SimpleMultithreadedParquetMerger;
import kz.hustle.tools.merge.MergingNotCompletedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.mockito.Mockito.*;

import static org.junit.Assert.*;

public class SimpleMultithreadedParquetMergerTest {
    private static Logger logger = Logger.getLogger(SimpleMultithreadedParquetMergerTest.class);

    private SimpleMultithreadedParquetMerger simpleMultithreadedParquetMerger;

    @BeforeClass
    public static void beforeTests() {
        logger.info("@BeforeClass");
    }

    @Before
    public void before() {
        logger.info("@Before");
        Configuration configuration = mock(Configuration.class);
        ParquetFolder parquetFolder = new ParquetFolder(new Path("/hdfs/mock/inputPath"), configuration);
        this.simpleMultithreadedParquetMerger = SimpleMultithreadedParquetMerger
                .builder(parquetFolder)
                .withOutputPath("hdfs/mock/outputPath")
                .withOutputFileName("awesomeFileName")
                .withCompressionCodec(CompressionCodecName.GZIP)
                .withThreadPoolSize(64)
                .withOutputRowGroupSize(128 * 1024 * 1024)
                .withInputChunkSize(128 * 1024 * 1024)
                .withBadBlockReadAttempts(5)
                .withBadBlockReadTimeout(30000)
                .build();
    }

    @Test
    public void builderTest() {
        logger.info("Test 1: Builder test");
        assertEquals("outputPath incorrect", this.simpleMultithreadedParquetMerger.getOutputPath(), "hdfs/mock/outputPath");
        assertEquals("compressionCodecName incorrect", this.simpleMultithreadedParquetMerger.getCompressionCodecName(), CompressionCodecName.GZIP);
        assertEquals("threadPoolSize incorrect", 64, this.simpleMultithreadedParquetMerger.getThreadPoolSize());
        assertEquals("outputRowGroupSize incorrect", 128*1024*1024, this.simpleMultithreadedParquetMerger.getOutputRowGroupSize());
        assertEquals("inputChunkSize incorrect", 128*1024*1024, this.simpleMultithreadedParquetMerger.getInputChunkSize());
        assertEquals("badBlockReadAttempts incorrect", 5, this.simpleMultithreadedParquetMerger.getBadBlockReadAttempts());
        assertEquals("badBloackReadTimeout incorrect", 30000, this.simpleMultithreadedParquetMerger.getBadBlockReadTimeout());
    }

    @Ignore
    @Test(expected = FileNotFoundException.class)
    public void mergeTestWithFileNotFoundException() throws IOException, MergingNotCompletedException, InterruptedException {
        logger.info("Test 2");
        Configuration configuration = mock(Configuration.class);
        ParquetFolder parquetFolder = new ParquetFolder(new Path("/hdfs/mock/inputPath"), configuration);
        this.simpleMultithreadedParquetMerger = SimpleMultithreadedParquetMerger
                .builder(parquetFolder)
                .build();
        FileSystem fs = mock(FileSystem.class);
        when (fs.exists(new Path("/hdfs/mock/inputPath"))).thenReturn(true);
        simpleMultithreadedParquetMerger.merge();
    }

    @Test
    public void mergeTest() {
        SimpleMultithreadedParquetMerger mockMerger = mock(SimpleMultithreadedParquetMerger.class);
    }

}
