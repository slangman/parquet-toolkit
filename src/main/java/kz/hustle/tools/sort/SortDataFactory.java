package kz.hustle.tools.sort;

/*import kz.dmc.packages.console.DMCConsoleColors;
import kz.dmc.packages.datetime.DMCDateTimeUtils;
import kz.dmc.packages.error.DMCError;
import net.minidev.json.JSONUtil;*/
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.DecoderException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.security.NoSuchProviderException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;

public class SortDataFactory {
    private Configuration conf;
    private FileSystem fs;
    private final SortThreadPool pool = ThreadPoolContainer.get().getPool("partsPool").setSize(1);
    private final Records records = new Records();
    private boolean isSort = true;
    private long recordsPerFile;
    private Schema schema;
    private String savePath;

    public SortDataFactory(Configuration conf) throws ClassNotFoundException, SQLException, IOException, NoSuchProviderException, DecoderException {
        this.conf = conf;
        this.fs = DistributedFileSystem.get(conf);
    }

    public void setRecordsPerFile(long recordsPerFile) {
        this.recordsPerFile = recordsPerFile;
    }

    public void setSort(boolean sort) {
        isSort = sort;
    }

    public void setSchema (Schema schema) {
        this.schema = schema;
    }

    public void setSavePath (String savePath) {
        this.savePath = savePath;
    }

    public void sortData() throws InterruptedException, ClassNotFoundException, SQLException, NoSuchProviderException, IOException, DecoderException {
        PartsProcess partsProcess = new PartsProcess();
        partsProcess.setSort(isSort);
        pool.addToPool(partsProcess);
        pool.shutDownAndWait();
    }

    private class PartsProcess implements Runnable {
        private final SortThreadPool pool = ThreadPoolContainer.get().getPool("sortPool").setPoolId(100);
        private final Schema schema = SortDataFactory.this.schema;
        //private final DMCDateTimeUtils time = new DMCDateTimeUtils().setStartWorkTime();
        private final DMCMemoryData memoryData = new DMCMemoryData();

        private Queue<byte[]> queueBytes = null;

        private boolean isSort = true;
        private boolean isFinish = false;

        private PartsProcess() throws ClassNotFoundException, SQLException, IOException, NoSuchProviderException, DecoderException {
        }

        public void setSort(boolean sort) {
            isSort = sort;
        }

        public boolean isFinish() {
            return isFinish;
        }

        @Override
        public void run() {
            String _sql;
            if (isSort) {
                _sql = "SELECT DATA FROM CACHE ORDER BY KEY";
                System.out.println("Сортировка...");
                //System.out.println(DMCConsoleColors.colorYellowText("Сортировка..."));
            } else {
                _sql = "SELECT DATA FROM CACHE";
                System.out.println("Сортировка не требуется...");
                //System.out.println(DMCConsoleColors.colorYellowText("Сортировка не требуется..."));
            }

            try {
                ResultSet _rs = memoryData
                        .connectToCache()
                        .getConnection()
                        .createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
                        .executeQuery(_sql);

                if (isSort) {
                    System.out.println("Сортировка завершена: ");
                    //System.out.println(DMCConsoleColors.colorGreenText("Сортировка завершена: " + time.getWorkTimeShort()));
                }

                queueBytes = new LinkedList<>();
                int threadID = 0;
                int dataSize = 0;
                int records = 0;
                /**********************************************************************
                 145 разница в сжатии GZIP и GZIP в паркете (Поправка на сжатие)
                 **********************************************************************/
                int bytesIn128MB = 145 * 1024 * 1024;
                //TODO:
                while (_rs.next()) {
                    records++;
                    queueBytes.add(_rs.getBytes("DATA"));

                    if (records >= recordsPerFile) {
                        records = 0;
                        threadID++;
                        /******************************************************************************************************/
                        SortProcess sortProcess = new SortProcess(queueBytes, schema, threadID).setThreadID(threadID);
                        this.pool.addToPool(sortProcess);
                        /******************************************************************************************************/
                        queueBytes = new LinkedList<>();
                        while (this.pool.getQueueTaskCount() >= 5) {
                            System.out.println("Active=" + pool.getActiveTaskCount() + " / Queue=" + pool.getQueueTaskCount());
                            Thread.sleep(1000);
                        }
                    }
                }

                _rs.close();

                if (queueBytes.size() > 0) {
                    threadID++;
                    System.out.println("threadID = " + threadID);
                    SortProcess sortProcess = new SortProcess(queueBytes, schema, threadID).setThreadID(threadID);
                    this.pool.addToPool(sortProcess);
                }

                memoryData.disconnectFromCache();

                try {
                    this.pool.shutDownAndWait();
                    System.out.println("Данные в памяти обработаны...");
                    //System.out.println(DMCConsoleColors.colorYellowText("Данные в памяти обработаны..."));
                    isFinish = true;
                } catch (Exception e) {
                    e.printStackTrace();
                    //System.out.println(DMCConsoleColors.colorRedText(DMCError.get().getFullErrorText(e)));
                    throw new RuntimeException(e);
                }

                /*FileStatus[] rawFiles = DMCController.get()
                        .getFileSystem()
                        .listFilesStatuses(savePath, ".parq");*/

            } catch (Exception e) {
                e.printStackTrace();
                //System.out.println(DMCConsoleColors.colorRedText(DMCError.get().getFullErrorText(e)));
                throw new RuntimeException(e);
            } finally {
                try {
                    this.pool.shutDownAndWait();
                    System.out.println("Данные в памяти обработаны...");
                    //System.out.println(DMCConsoleColors.colorYellowText("Данные в памяти обработаны..."));
                    isFinish = true;
                } catch (Exception e) {
                    e.printStackTrace();
                    //System.out.println(DMCConsoleColors.colorRedText(DMCError.get().getFullErrorText(e)));
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private class Records {
        private final Queue<SortData> records = new LinkedList<>();
        private boolean isFillStart = false;
        private boolean isFinish = false;

        public Queue<SortData> getRecords() {
            return records;
        }

        public SortData poll() {
            return records.poll();
        }

        public boolean isFinish() {
            return isFinish;
        }

        public void setFinish(boolean finish) {
            isFinish = finish;
        }

        public void setFillStart(boolean fillStart) {
            isFillStart = fillStart;
        }

        public int size() {
            return records.size();
        }

        public void add(SortData data) {
            records.add(data);
        }

        public void remove() {
            records.remove(0);
        }

        public SortData get() {
            return poll();
        }

        public SortData waitingAndGet() throws InterruptedException {

            while (!isFillStart) {
                Thread.sleep(100);
            }

            while (records.size() == 0) {
                System.out.println("records.size=" + records.size());
                Thread.sleep(100);
            }

            SortData data;

            while ((data = get()) == null) {
                System.out.print("\r" + "records.get(0)=NULL - records.size=" + records.size());
                Thread.sleep(100);
            }

            while (!data.isReady()) {
                System.out.println("data.isReady=" + data.isReady);
                Thread.sleep(100);
            }

            return data;
        }

    }

    private class SortData {
        private final Queue<GenericRecord> queue = new LinkedList<>();
        private int id = 0;
        private final int size = 0;
        private boolean isReady = false;

        public int size() {
            return queue.size();
        }

        public boolean isReady() {
            return isReady;
        }

        public void setReady(boolean ready) {
            isReady = ready;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public GenericRecord get() {
            if (!queue.isEmpty()) {
                GenericRecord _record = queue.poll();
                return _record;
            }

            return null;
        }

        public void add(GenericRecord record) {
            this.queue.add(record);
        }
    }

    private class SortProcess implements Runnable {
        private SortData sortData;
        private long threadID = 0;
        private Queue<byte[]> queue;
        private Schema schema;

        public SortProcess(Queue<byte[]> queue, Schema schema, int queueID) {
            this.queue = queue;
            this.schema = schema;
            this.sortData = new SortData();
            this.sortData.setId(queueID);
            records.add(sortData);
            records.setFillStart(true);
        }

        public SortProcess setThreadID(long threadID) {
            this.threadID = threadID;
            return this;
        }

        @Override
        public void run() {
            try {
                DefaultParquetFileWriter parquetFileWriter = new DefaultParquetFileWriter();
                if (threadID > 0) {
                    parquetFileWriter.setFilePart((int) threadID);
                }

                parquetFileWriter
                        .setHadoopFileSystem(fs)
                        .setConfiguration(conf)
                        .setSavePath(SortDataFactory.this.savePath)
                        .setBlockSize(128)
                        .setSchema(SortDataFactory.this.schema)
                        .createParquetFile();

                byte[] bytes;
                while ((bytes = this.queue.poll()) != null) {
                    parquetFileWriter.appendRecord(ParquetGenericRecordBinary.byteToRecord(ByteStreamCompressor.decompressGzip(bytes), schema));
                }

                parquetFileWriter.closeParquetFile();

                queue.clear();

                sortData.setReady(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
