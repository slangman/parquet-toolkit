package kz.hustle.test;

import kz.hustle.ParquetFolder;
import kz.hustle.tools.ParquetMerger;
import kz.hustle.tools.SimpleMultithreadedParquetMerger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class SimpleMultithreadParquetMergerTest {

    public static void main(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];

        Configuration configuration = ConfigurationBuilder.getHDFSConfigurationKerberos();
        UserGroupInformation.setConfiguration(configuration);
        UserGroupInformation.loginUserFromKeytab("DIvantsov@KAR-TEL.LOCAL", "C:\\ooo.key");

        ParquetFolder folder = new ParquetFolder(new Path(inputPath), configuration);
        ParquetMerger merger = SimpleMultithreadedParquetMerger.builder(folder)
                .withCompressionCodec(CompressionCodecName.GZIP)
                .withThreadPoolSize(64)
                .withOutputRowGroupSize(128 * 1024 * 1024)
                .withInputChunkSize(128 * 1024 * 1024)
                .withOutputPath(outputPath)
                .withInt96FieldsSupport()
                .build();
        merger.merge();
    }
}
