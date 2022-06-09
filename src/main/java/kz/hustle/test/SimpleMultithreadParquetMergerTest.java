package kz.hustle.test;

import kz.hustle.ParquetFolder;
import kz.hustle.tools.common.InputPath;
import kz.hustle.tools.common.InputSource;
import kz.hustle.tools.merge.ParquetMerger;
import kz.hustle.tools.merge.SimpleMultithreadedParquetMerger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class SimpleMultithreadParquetMergerTest {

    public static void main(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];

        Configuration configuration = ConfigurationBuilder.getClouderaQuickstartConf();

        InputSource inputSource = new InputPath(args[0]);

        ParquetMerger merger = SimpleMultithreadedParquetMerger.builder(configuration)
                .inputSource(inputSource)
                .compressionCodec(CompressionCodecName.GZIP)
                .threadPoolSize(4)
                .outputRowGroupSize(128 * 1024 * 1024)
                .outputChunkSizeMegabytes(128)
                .outputPath(outputPath)
                .removeInputAfterMerging(true, true)
                .withInt96FieldsSupport()
                .build();
        merger.merge();


        /*ParquetFolder folder = new ParquetFolder(new Path(inputPath), configuration);
        ParquetMerger merger = SimpleMultithreadedParquetMerger.builder(folder)
                .withCompressionCodec(CompressionCodecName.GZIP)
                .withThreadPoolSize(4)
                .withOutputRowGroupSize(128 * 1024 * 1024)
                .withInputChunkSize(128 * 1024 * 1024)
                .withOutputPath(outputPath)
                .withInt96FieldsSupport()
                .build();
        merger.merge();*/
    }
}
