package kz.hustle.test;

import kz.hustle.ParquetFile;
import kz.hustle.tools.sort.BigParquetSorter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class MainTest {
    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration configuration = ConfigurationBuilder.getClouderaQuickstartConf();

        String input = args[0];

        //String output = args[1];

        ParquetFile parquetFile = new ParquetFile(new Path(input), configuration);

        BigParquetSorter parquetSorter = BigParquetSorter.builder()
                .withConfiguration(configuration)
                .withInputPath(new Path(input))
                .build();

    }
}
