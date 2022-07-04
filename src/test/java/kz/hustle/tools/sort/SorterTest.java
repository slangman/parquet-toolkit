package kz.hustle.tools.sort;

import kz.hustle.tools.TestUtils;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.BeforeClass;

import java.io.IOException;

public abstract class SorterTest {

    protected static MiniDFSCluster cluster;
    protected static HdfsConfiguration conf;





    protected static void createMiniDFSCluster() {
        conf = new HdfsConfiguration();
        try {
            cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
