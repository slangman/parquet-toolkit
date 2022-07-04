package kz.hustle.tools.merge;

import kz.hustle.tools.TestUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class MergerTest {
    protected static HdfsConfiguration conf;
    protected static MiniDFSCluster cluster;
    protected static final Logger LOGGER = LoggerFactory.getLogger(MergerTest.class);

    protected static final String TEST_INPUT_PATH = "/test/merge/";
    protected static final String TEST_OUTPUT_PATH = "/test/result/";
    protected static final int NUMBER_OF_FILES = 10;
    protected static final int RECORDS_IN_FILE = 10;
    protected static final int THREAD_POOL_SIZE = 4;
    protected static final int OUTPUT_CHUNK_SIZE_MEGABYTES = 128;
    protected static final int BAD_BLOCKS_READ_ATTEMPTS = 4;
    protected static final int BAD_BLOCK_READ_TIMEOUT = 29999;

    protected static void createResources() {
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
    }


}
