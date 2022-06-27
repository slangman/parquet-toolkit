package kz.hustle.tools.split;

import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class SplitterTest {
    protected static final int OUTPUT_CHUNK_SIZE = 2 * 1024 * 1024;
    protected static final int OUTPUT_ROW_GROUP_SIZE = 1024 * 1024;
    protected static final int THREAD_POOL_SIZE = 4;
    protected static final int RECORDS_IN_FILE = 1000000;
    protected static final String TEST_INPUT_PATH = "/test/split/file-to-split.parquet";
    protected static final String TEST_OUTPUT_PATH = "test/split-result/";
    protected static final Logger LOGGER = LoggerFactory.getLogger(SplitterTest.class);

    protected static HdfsConfiguration conf;
    protected static MiniDFSCluster cluster;

    protected static void createMiniDFSCluster() {
        conf = new HdfsConfiguration();
        try {
            cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
