package kz.hustle.test;

import kz.hustle.ParquetFolder;
import kz.hustle.tools.TreeMultithreadedParquetMerger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class TreeMultithreadedParquetMergerTest {

    public static void main(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];

        Configuration configuration = ConfigurationBuilder.getHDFSConfiguration();

        ParquetFolder folder = new ParquetFolder(new Path(inputPath), configuration);

        TreeMultithreadedParquetMerger.builder(folder)
                .withOutputPath(new Path(outputPath))
                .build()
                .merge();
    }


}
