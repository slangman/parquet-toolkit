package kz.hustle.tools;

import kz.hustle.tools.sort.*;
import kz.hustle.tools.merge.MergeUtils;
import kz.hustle.tools.sort.*;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class BigParquetSorter extends BaseParquetSorter {

    private int numberOfFilesProcessedInParallel;
    private int submitThreadCount;
    private int saveThreadCount;
    private long batchSize;
    private FileStatus[] files;

    //непонятная фигня, придуманная Андреем
    private boolean isSort = true;


    public BigParquetSorter(Builder builder) throws IOException {
        super(builder);
        this.numberOfFilesProcessedInParallel = builder.numberOfFilesProcessedInParallel;
        this.submitThreadCount = builder.submitThreadCount;
        this.saveThreadCount = builder.saveThreadCount;
        this.batchSize = builder.batchSize;
        files = fs.listStatus(inputPath);
        if (fileFilter != null) {
            files = Arrays.stream(files).filter(e->e.getPath().getName().endsWith(fileFilter)).toArray(FileStatus[]::new);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends BaseParquetSorter.Builder<Builder> {

        private int numberOfFilesProcessedInParallel = 16;
        private int submitThreadCount = 64;
        private int saveThreadCount = 64;
        private long batchSize = 100000;
        private int unnecessaryTestField;

        @Override
        public Builder getThis() {
            return this;
        }


        public Builder withNumberOfFilesProcessedInParallel(int numberOfFilesProcessedInParallel) {
            this.numberOfFilesProcessedInParallel = numberOfFilesProcessedInParallel;
            return this;
        }

        public Builder withSubmitThreadCount(int submitThreadCount) {
            this.submitThreadCount = submitThreadCount;
            return this;
        }

        public Builder withSaveThreadCount(int saveThreadCount) {
            this.saveThreadCount = saveThreadCount;
            return this;
        }

        public Builder withBatchSize(long batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder withUnnecessaryTestField() {
            this.unnecessaryTestField = 42;
            return this;
        }

        @Override
        public BigParquetSorter build() throws IOException {
            return new BigParquetSorter(this);
        }
    }

    @Override
    public void sort(String field) throws Exception {
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        List<String> arguments = runtimeMxBean.getInputArguments();
        ThreadPoolContainer.get().getPool("factoryPool").setSize(numberOfFilesProcessedInParallel);
        ThreadPoolContainer.get().getPool("partPool").setSize(submitThreadCount);
        ThreadPoolContainer.get().getPool("sortPool").setSize(saveThreadCount);

        DMCMemoryData memoryData = new DMCMemoryData();
        memoryData.connectToCache();
        isSort = field != null;
        FilesFactory filesFactory = new FilesFactory(new DefaultFileProcessor(field, conf));
        try (DefaultParquetFileReader parquetFileReader = new DefaultParquetFileReader().setConf(conf).open(files[0]);) {
            schema = parquetFileReader.getSchema();
        }

        //DMCVariablesContainer.get().setVariable("schema", fileSchema);
        //DMCVariablesContainer.get().setVariable("savePath", DMCPath.setPathDelimeter(DMCPath.setPathDelimeter("/tmp/sort-parquet-files/") + DMCUtils.get().getUniqueID()));
        String savePath = inputPath.toString() + "/_tmp/" + UUID.randomUUID().toString().toUpperCase();
        //System.out.println(DMCConsoleColors.colorGreenText("Временная папка: " + DMCVariablesContainer.get().getVariable("savePath")));

        filesFactory
                .setFiles(files)
                .setParallelFilesProcessing(numberOfFilesProcessedInParallel)
                .setPartFileSize(batchSize)
                .setProcessorsThreadPool(ThreadPoolContainer.get().getPool("factoryPool"))
                .setPartFilePool(ThreadPoolContainer.get().getPool("partPool"));

        long start = System.currentTimeMillis();

        filesFactory.factoryLaunch();

        SortDataFactory sortDataFactory = new SortDataFactory(conf);
        sortDataFactory.setSchema(schema);
        sortDataFactory.setRecordsPerFile(batchSize);
        sortDataFactory.setSort(isSort);
        sortDataFactory.setSavePath(savePath);
        sortDataFactory.sortData();
        System.out.println("Processing complete in " + MergeUtils.getWorkTime(System.currentTimeMillis() - start));
    }

    private boolean vmSettingsIsOk() {
        //long fileSize = fs.
        return false;
    }
}
