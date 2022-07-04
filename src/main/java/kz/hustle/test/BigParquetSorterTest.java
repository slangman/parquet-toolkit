package kz.hustle.test;

import kz.hustle.tools.sort.BigParquetSorter;
import kz.hustle.tools.sort.ParquetSorter;
import org.apache.hadoop.fs.Path;


//{"date":"17.05.2021",
// "source-file-expression":"@format,[yyyy-MM-dd]",
// "files-thread":16,
// "submit-thread-count":128,
// "save-thread-count":64,
// "batch-size":100000,
// "file-filter":".parq",
// "sort-field":"SUBS_KEY",
// "sort-type":"String",
// "test":false}
public class BigParquetSorterTest {
    public static void main(String[] args) throws Exception {
        ParquetSorter sorter = BigParquetSorter.builder()
                .withInputPath(new Path(args[0]))
                .withNumberOfFilesProcessedInParallel(16)
                .withSubmitThreadCount(64)
                .withSaveThreadCount(64)
                .withBatchSize(10000)
                .withFileFilter(".parquet")
                .build();
        sorter.sort("subs_key");
    }
}
