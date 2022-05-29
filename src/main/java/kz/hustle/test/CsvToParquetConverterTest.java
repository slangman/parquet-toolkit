package kz.hustle.test;

import kz.hustle.tools.convert.CsvToParquetConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class CsvToParquetConverterTest {
    public static void main(String[] args) throws Exception {

        String inputPath = args[0];
        String outputPath = args[1];

        Configuration configuration = ConfigurationBuilder.getClouderaQuickstartConf();

        CsvToParquetConverter converter = CsvToParquetConverter.builder()
                .withInputPath(inputPath)
                .withOutputPath(outputPath)
                .withConf(configuration)
                .withDelimiter(';')
                .withThreadPoolSize(64)
                .withLinesPerThread(500000)
                .withCompressionCodec(CompressionCodecName.GZIP)
                .withFirstRecordAsHeader()
                .withSkipFirstLine()
                .build();
        converter.convert();
    }
}
