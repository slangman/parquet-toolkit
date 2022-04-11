package kz.hustle.tools.sort;

import kz.dmc.packages.console.DMCConsoleColors;
import kz.dmc.packages.controllers.DMCController;
import kz.dmc.packages.error.DMCError;
import kz.dmc.packages.threads.pools.DMCThreadsPool;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.LinkedList;

public class DefaultFileProcessor implements FileProcessor {

    private String sortColumn;
    private SortThreadPool factoryPool;
    private SortThreadPool partFilePool = null;
    private long partFileSize = 0;
    private DefaultParquetFileReader reader = null;
    private long allRecords = 0L;
    private Configuration conf;

    public DefaultFileProcessor(String sortColumn, Configuration conf) {
        this.sortColumn = sortColumn;
        this.conf = conf;
        this.reader = new DefaultParquetFileReader();
        this.reader.setConf(conf);
    }

    @Override
    public void execute(FileStatus file) throws IOException {
        allRecords += reader.getRecordsCount(file);
        Processor processor = new Processor(file, sortColumn);
        factoryPool.addToPool(processor);
    }

    @Override
    public void setFactoryPool(SortThreadPool pool) {
        this.factoryPool = pool;
    }

    @Override
    public void setPartFilePool(SortThreadPool pool) {
        this.partFilePool = pool;
    }

    @Override
    public void setPartFileSize(long size) {
        this.partFileSize = partFileSize;
    }

    private class Processor implements Runnable {
        private DefaultParquetFileReader reader = null;
        private FileStatus file;
        private String sortColumn;

        public Processor(FileStatus file, String sortColumn) {
            this.file = file;
            this.sortColumn = sortColumn;
        }

        @Override
        public void run() {
            try {
                this.reader = new DefaultParquetFileReader();
                reader.setConf(DefaultFileProcessor.this.conf);
                reader.open(file);
                GenericRecord record;
                LinkedList<GenericRecord> recordsList = new LinkedList<>();
                int currentReccondIndex = 0;
                while ((record = reader.getRecord()) != null) {
                    currentReccondIndex++;
                    recordsList.addFirst(record);
                    if (currentReccondIndex >= partFileSize) {
                        currentReccondIndex = 0;
                        LoadPart part = new LoadPart(new WeakReference<>(recordsList), sortColumn);
                        partFilePool.addToPool(part);
                        while (partFilePool.getQueueTaskCount() >= 5) {
                            Thread.sleep(500);
                        }
                        recordsList = new LinkedList<>();
                    }
                }
                reader.close();
                if (recordsList.size() > 0) {
                    LoadPart part = new LoadPart(new WeakReference<>(recordsList), sortColumn);
                    partFilePool.addToPool(part);
                }
            } catch (Exception e) {
                System.out.println(DMCConsoleColors.colorRedText(DMCError.get().getFullErrorText(e)));
            }
        }
    }

    private class LoadPart implements Runnable {
        private DMCMemoryData memoryData = null;
        private LinkedList<GenericRecord> recordsList = null;

        private String sortColumn = null;

        public LoadPart(WeakReference<LinkedList<GenericRecord>> recordsList, String sortColumn) {
            this.recordsList = recordsList.get();
            this.sortColumn = sortColumn;
            this.memoryData = new DMCMemoryData();
        }


        @Override
        public void run() {
            try {

                memoryData.setSortColumn(sortColumn);
                memoryData.connectToCache().creteCache();
                memoryData.appendData(recordsList);
                memoryData.disconnectFromCache();

                recordsList.clear();

            } catch (Exception e) {
//                e.printStackTrace();
                System.out.println(DMCError.get().getFullErrorText(e));
                System.exit(0);
            }
        }
    }
}
