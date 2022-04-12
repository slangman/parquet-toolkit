package kz.hustle.test;

import kz.hustle.ParquetFile;
import kz.hustle.tools.convert.CsvToParquetConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

//Только для тестирования, в сборку не включать
public class Main {
    public static void main(String[] args) throws Exception {

        String inputPath = args[0];
        String outputPath = args[1];

        Configuration configuration = ConfigurationBuilder.getHDFSConfiguration();

        String[] header = {"Consecutive_Number","Name","Distinguished_Name","Alarm_Type","Alarm_Number","Severity",
                "Alarm_Time","Cancel_Time","Cancel_User","Acknowledgement_State","Acknowledgement_Time_UnAcknowledgement_Time",
                "Acknowledgement_User","Notification_ID","Alarm_Text","Extra_Information","User_Additional_Information",
                "Supplementary_Information","Diagnostic_Info","Additional_Information_1","Additional_Information_2","Additional_Information_3",
                "Probable_Cause","Correlated_Alarm","Correlation_Indicator","Adaptation_ID","Adaptation_Release","Object_Class_Controlling_object_class",
                "Instance_Counter"};

        CsvToParquetConverter converter = CsvToParquetConverter.builder()
                .withInputPath(inputPath)
                .withOutputPath(outputPath)
                .withConf(configuration)
                .withDelimiter(',')
                .withQuote('^')
                .withThreadPoolSize(64)
                .withLinesPerThread(500000)
                .withCompressionCodec(CompressionCodecName.GZIP)
                .withHeader(header)
                .withSkipFirstLine()
                .build();
        converter.convert();

        ParquetFile file = new ParquetFile(new Path(args[0]), configuration);



        //FileSystem fs = DistributedFileSystem.get(configuration);

        /*ParquetFile file = new ParquetFile(new Path(args[0]), configuration);
        MultithreadParquetSplitter splitter = MultithreadParquetSplitter.builder(file)
                .withOutputChunkSize(128*1024*1024)
                .withCompressionCodec(CompressionCodecName.GZIP)
                .withThreadPoolSize(16)
                .withOutputPath(new Path("/dmp/test/test-path"))
                .build();
        splitter.split();*/



        /*try (Stream<String> stream = Files.lines(Paths.get(args[0]))) {
            stream.forEach(inputPath -> {
                if (!StringUtils.isBlank(inputPath)) {
                    try {
                        List<FileStatus> files = Arrays
                                .stream(fs.listStatus(new Path(inputPath)))
                                .filter(e -> e.getPath().toString().contains("csv"))
                                .collect(Collectors.toList());
                        for (FileStatus file : files) {
                            String fileInputPath = file.getPath().toString();
                            String fileName = file.getPath().getName();
                            String folder = fileInputPath.substring(0, fileInputPath.lastIndexOf("/"));
                            CsvToParquetConverter converter = CsvToParquetConverter.builder()
                                    .withInputPath(fileInputPath)
                                    .withOutputPath("/bigdata/datamart/datamart_seb/daily/seb_parquet" + folder.substring(folder.lastIndexOf("/")) + "/" + fileName.substring(0, fileName.lastIndexOf(".")) + ".parq")
                                    .withConf(configuration)
                                    .withDelimiter(';')
                                    .withQuote('"')
                                    .withFirstRecordAsHeader()
                                    .withThreadPoolSize(64)
                                    .withLinesPerThread(500000)
                                    .build();
                            converter.convert();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }*/
    }
}
