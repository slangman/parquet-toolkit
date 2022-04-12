package kz.hustle.tools.convert;

import kz.hustle.ParquetFolder;
import kz.hustle.tools.merge.ParquetMerger;
import kz.hustle.tools.merge.SimpleMultithreadedParquetMerger;
import kz.hustle.tools.common.ThreadPool;
import kz.hustle.tools.merge.MergingNotCompletedException;
import kz.hustle.utils.DefaultConfigurationBuilder;
import kz.hustle.utils.WorkTime;
import org.apache.avro.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.zip.GZIPInputStream;

//TODO: Добавить опцию сохранения пустых файлов
public class CsvToParquetConverter {
    private Configuration conf;
    private FileSystem fs;
    private ThreadPool threadPool;
    private String inputPath;
    private String outputPath;
    private String tempPath;
    private String[] header;
    private String schemaName;
    private Schema schema;
    private boolean skipFirstLine;
    private boolean firstRecordAsHeader;
    private boolean allowMissingColumnNames;
    private boolean skipLastLine;
    private int linesPerThread = 100000;
    private int poolSize = 64;
    private Queue<String> lines = null;
    private Queue<CSVRecord> records = null;
    private int linesCounter;
    private int fileIndex = 0;
    private boolean hasErrors;
    private char delimiter = ',';
    private char quote = '"';
    private CompressionCodecName compressionCodecName;
    private LinkedHashMap<String, Schema.Type> fieldTypes;
    private String recordSeparator;
    private boolean saveEmptyFile;
    private int linesToSkip = 0;
    private boolean forConverse15Min;

    private CsvToParquetConverter() {
    }

    /**
     * @param inputPath
     * @param outputPath If this param is null, converted file is saved in default path (path/to/input/file/converted)
     * @param header     If this param is null, the program attempts to get header from input file.
     *                   In that case you must be sure that input file has header.
     * @param schemaName If this param is null, the schema name will be 'default'.
     * @param conf
     * @throws IOException
     */
    @Deprecated
    public CsvToParquetConverter(String inputPath,
                                 String outputPath,
                                 String[] header,
                                 String schemaName,
                                 Configuration conf,
                                 boolean firstRecordAsHeader,
                                 int linesPerThread,
                                 int poolSize) throws Exception {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.header = header;
        this.schemaName = schemaName;
        this.conf = DefaultConfigurationBuilder.getConf();
        this.fs = DistributedFileSystem.get(this.conf);
        this.firstRecordAsHeader = firstRecordAsHeader;
        this.linesPerThread = linesPerThread;
        this.poolSize = poolSize;
    }

    @Deprecated
    public CsvToParquetConverter(String inputPath,
                                 String outputPath,
                                 String[] header,
                                 String schemaName,
                                 Configuration conf,
                                 boolean firstRecordAsHeader) throws Exception {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.header = header;
        this.schemaName = schemaName;
        this.conf = conf;
        this.fs = DistributedFileSystem.get(this.conf);
        this.firstRecordAsHeader = firstRecordAsHeader;
    }

    @Deprecated
    private CsvToParquetConverter(String inputPath, String outputPath, Schema schema, Configuration conf, boolean skipFirstLine) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.schema = schema;
        this.conf = conf;
        this.skipFirstLine = skipFirstLine;
    }

    public static CsvToParquetConverter.Builder builder() throws IOException {
        return new CsvToParquetConverter().new Builder();
    }

    public class Builder {
        private Builder() throws IOException {
            /*CsvToParquetConverter.this.conf = DefaultConfigurationBuilder.getConf();
            CsvToParquetConverter.this.fs = DistributedFileSystem.get(CsvToParquetConverter.this.conf);*/
        }

        public Builder withInputPath(String inputPath) {
            CsvToParquetConverter.this.inputPath = inputPath;
            return this;
        }

        public Builder withOutputPath(String outputPath) {
            CsvToParquetConverter.this.outputPath = outputPath;
            return this;
        }

        public Builder withConf(Configuration conf) {
            CsvToParquetConverter.this.conf = conf;
            return this;
        }

        public Builder withSchema(Schema schema) {
            CsvToParquetConverter.this.schema = schema;
            return this;
        }

        public Builder withCustomFieldTypes(LinkedHashMap<String, Schema.Type> fieldTypes) {
            CsvToParquetConverter.this.fieldTypes = fieldTypes;
            return this;
        }

        public Builder withSchemaName(String schemaName) {
            CsvToParquetConverter.this.schemaName = schemaName;
            return this;
        }

        public Builder withLinesPerThread(int linesPerThread) {
            CsvToParquetConverter.this.linesPerThread = linesPerThread;
            return this;
        }

        public Builder withThreadPoolSize(int threadPoolSize) {
            CsvToParquetConverter.this.poolSize = threadPoolSize;
            return this;
        }

        public Builder withDelimiter(char delimiter) {
            CsvToParquetConverter.this.delimiter = delimiter;
            return this;
        }

        public Builder withQuote(char quote) {
            CsvToParquetConverter.this.quote = quote;
            return this;
        }

        public Builder withHeader(String[] header) {
            CsvToParquetConverter.this.header = header;
            return this;
        }

        public Builder withSkipFirstLine() {
            CsvToParquetConverter.this.skipFirstLine = true;
            return this;
        }

        public Builder withSkipLastLine() {
            CsvToParquetConverter.this.skipLastLine = true;
            return this;
        }

        public Builder withFirstRecordAsHeader() {
            CsvToParquetConverter.this.firstRecordAsHeader = true;
            return this;
        }

        public Builder withCompressionCodec(CompressionCodecName compressionCodecName) {
            CsvToParquetConverter.this.compressionCodecName = compressionCodecName;
            return this;
        }

        /**
         * Позволяет обрабатывать заголовки с пустыми именами полей
         *
         * @return Объект Builder, идентичный имеющемуся, но позволяющицй конвертеру обрабатывать заголовки
         * с пустыми именами полей.
         */
        public Builder withAllowMissingColumnNames() {
            CsvToParquetConverter.this.allowMissingColumnNames = true;
            return this;
        }

        /**
         * Устанавливает определенный разделитель записей для обрабатываемого .csv файла
         *
         * <p>
         * <strong>Note:</strong> Допустимые значения разделителя: '\n', '\r' и "\r\n".
         * Все прочие значения не будут приводить к ошибке, но будут игнорироваться.
         * </p>
         *
         * @param recordSeparator разделитель записей
         * @return Объект Builder, идентичный имеющемуся, но позволяющий конвертеру обрабатывать .csv файл
         * с определенным разделителем.
         */
        public Builder withRecordSeparator(String recordSeparator) {
            CsvToParquetConverter.this.recordSeparator = recordSeparator;
            return this;
        }

        /**
         * Позволяяет пропустить определенное количество строк в начале файла.
         * Если файл начниается с пустых строк, отсчёт начинается с первой непустой строки.
         *
         * @param linesToSkip - количество строк, которые требуется пропустить
         * @return Объект Builder, идентичный имеющемуся, но позволяющий конвертеру пропускать
         * определенное количество строк в начале файла
         */
        public Builder withSkipLinesInBeginning(int linesToSkip) {
            CsvToParquetConverter.this.linesToSkip = linesToSkip;
            return this;
        }

        public Builder forConverse15Min() {
            CsvToParquetConverter.this.forConverse15Min = true;
            return this;
        }

        public CsvToParquetConverter build() {
            return CsvToParquetConverter.this;
        }
    }

    public void convert() throws Exception {
        Path p = new Path(inputPath);
        fs = p.getFileSystem(conf);
        if (header == null && fieldTypes == null && !firstRecordAsHeader) {
            throw new Exception("Header not provided");
        }
        if (fieldTypes != null) {
            buildSchema(fieldTypes);
        }
        if (schema == null) {
            buildSchema(header);
        }
        if (schema == null) {
            return;
        }
        WorkTime workTime = new WorkTime();
        workTime.startTime();
        if (outputPath == null) {
            setOutputPath();
        }
        if (compressionCodecName == null) {
            compressionCodecName = CompressionCodecName.GZIP;
        }
        setTempPath();
        threadPool = new ThreadPool(poolSize);
        try (InputStream inputStream = fs.open(new Path(inputPath), 4096 * 2)) {
            InputStreamReader reader;
            if (inputPath.endsWith(".gz")) {
                GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
                reader = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8);
            } else {
                reader = new InputStreamReader(inputStream);
            }
            List<CSVRecord> records = new ArrayList<>();
            //Iterable<CSVRecord> records;
            //TODO: Сделать потом нормально
            CSVFormat csvFormat = CSVFormat.DEFAULT.withDelimiter(delimiter).withQuote(quote);
            if (firstRecordAsHeader) {
                csvFormat = csvFormat.withFirstRecordAsHeader();
            } else {
                csvFormat = csvFormat.withHeader(header);
            }
            if (allowMissingColumnNames) {
                csvFormat = csvFormat.withAllowMissingColumnNames();
            }
            if (recordSeparator != null) {
                csvFormat = csvFormat.withRecordSeparator(recordSeparator);
            }
            if (skipFirstLine) {
                csvFormat = csvFormat.withSkipHeaderRecord();
            }
            csvFormat.parse(reader).forEach(records::add);
            //records = csvFormat.parse(reader);
            if (skipLastLine) {
                records.remove(records.size() - 1);
            }
            if (linesToSkip > 0) {
                for (int i = 0; i < linesToSkip; i++) {
                    if (records.iterator().hasNext()) {
                        records.iterator().next();
                    }
                }
            }
            if (records.iterator().hasNext()) {
                records.forEach(this::processRecord);
                residualRecords();
                reader.close();
            } else {
                System.out.println("Input file is empty. Parquet file is not created.");
                threadPool.shutDown();
                threadPool.await();
                return;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        threadPool.shutDown();
        threadPool.await();
        if (records != null) {
            records.clear();
        }
        mergeTempFiles();
        System.out.println("File " + inputPath + " converted in " + workTime.getWorkTimeShort());
    }

    public void mergeTempFiles() throws IOException, InterruptedException, MergingNotCompletedException {
        Path tp = new Path(tempPath);
        FileStatus[] fileStatuses = fs.listStatus(tp);
        if (fileStatuses.length == 1) {
            String destDir = outputPath.substring(0, outputPath.lastIndexOf("/"));
            if (!fs.exists(new Path(destDir))) {
                fs.mkdirs(new Path(destDir));
            }
            if (fs.rename(fileStatuses[0].getPath(), new Path(outputPath))) {
                System.out.println(fileStatuses[0].getPath().toString() + " moved to " + outputPath);
            }
        } else {
            ParquetMerger merger = SimpleMultithreadedParquetMerger.builder(new ParquetFolder(tp, conf))
                    .withOutputRowGroupSize(128 * 1024 * 1024)
                    .withInputChunkSize(128 * 1024 * 1024)
                    .withThreadPoolSize(poolSize)
                    .withCompressionCodec(compressionCodecName)
                    .withOutputPath(outputPath.substring(0, outputPath.lastIndexOf("/")))
                    .withOutputFileName(outputPath.substring(outputPath.lastIndexOf("/") + 1, outputPath.lastIndexOf(".")))
                    .withRemoveInputFiles()
                    .build();
            merger.merge();
        }
        if (fs.listStatus(tp).length == 0) {
            fs.delete(tp, true);
        }
    }

    private void buildSchema(String[] header) throws IOException {
        if (header == null) {
            setHeader();
        }
        if (this.header == null) {
            return;
        }
        if (schemaName == null) {
            schemaName = "default";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("{")
                .append("  \"type\": \"record\",")
                .append("  \"name\": \"" + schemaName + "\",")
                .append("  \"doc\": \"Схема файла " + schemaName + "\",")
                .append("  \"fields\": [");

        Arrays.stream(this.header).forEach(i -> {
                    if (!StringUtils.isBlank(i)) {
                        sb.append("{\"name\":\"")
                                .append(i)
                                .append("\",\"type\":[\"null\",\"string\"],\"default\": null},");
                    }
                }
        );
        sb.deleteCharAt(sb.lastIndexOf(","));
        sb.append("  ]}");
        schema = new Schema.Parser().parse(sb.toString());
    }

    private void buildSchema(LinkedHashMap<String, Schema.Type> fieldTypes) {
        this.header = new String[fieldTypes.size()];
        int[] counter = {0};
        fieldTypes.forEach((k, v) -> {
            this.header[counter[0]] = k;
            counter[0]++;
        });
        if (schemaName == null) {
            schemaName = "default";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("{")
                .append("  \"type\": \"record\",")
                .append("  \"name\": \"" + schemaName + "\",")
                .append("  \"doc\": \"Схема файла " + schemaName + "\",")
                .append("  \"fields\": [");

        fieldTypes.forEach((k, v) -> {
            sb.append("{\"name\":\"")
                    .append(k)
                    .append("\",\"type\":[\"null\",\"")
                    .append(v.name().toLowerCase())
                    .append("\"],\"default\": null},");
        });
        sb.deleteCharAt(sb.lastIndexOf(","));
        sb.append("  ]}");
        schema = new Schema.Parser().parse(sb.toString());
    }

    private void setHeader() {
        try (InputStream inputStream = fs.open(new Path(inputPath), 4096 * 2);) {
            BufferedReader br;
            if (inputPath.endsWith(".gz")) {
                GZIPInputStream gzip = new GZIPInputStream(inputStream);
                br = new BufferedReader(new InputStreamReader(gzip));
            } else {
                br = new BufferedReader(new InputStreamReader(inputStream));
            }
            header = br.readLine().split(new String(new char[]{delimiter}));
            skipFirstLine = true;
            br.close();
        } catch (Exception e) {
            header = null;
        }
    }

    private void setOutputPath() {
        outputPath = inputPath
                .substring(0, inputPath.lastIndexOf("/") + 1)
                + "converted/"
                + inputPath.substring(inputPath.lastIndexOf("/") + 1);
    }

    private void processRecord(CSVRecord record) {
        linesCounter++;
        if (linesCounter == 1) {
            records = new ArrayBlockingQueue<>(linesPerThread);
            records.add(record);
        } else if (linesCounter == linesPerThread) {
            records.add(record);
            String outFileName = generateSavePathWithPart(fileIndex++);
            while (threadPool.getQueueSize() > 1) {
            }
            threadPool.addNewThread(new ConvertThread(records, outFileName, schema, conf, compressionCodecName, fieldTypes));
            System.out.println("Thread " + fileIndex + " created");
            records = new ArrayBlockingQueue<>(linesPerThread);
            linesCounter = 0;
        } else {
            records.add(record);
        }
    }

    private void residualRecords() {
        if (records != null && records.size() > 0) {
            String outFileName = generateSavePathWithPart(fileIndex++);
            threadPool.addNewThread(new ConvertThread(records, outFileName, schema, conf, compressionCodecName, fieldTypes));
            System.out.println("Thread " + fileIndex + " created");
        }
    }

    private void setTempPath() {
        tempPath = inputPath
                .substring(0, inputPath.lastIndexOf("/") + 1)
                + "_" + (outputPath.substring(outputPath.lastIndexOf("/") + 1));
    }

    private String generateSavePathWithPart(int index) {
        return tempPath
                + "/" + "part" + index + "_" + (outputPath.substring(outputPath.lastIndexOf("/") + 1));
    }

    private CSVFormat createCsvFormat() {
        CSVFormat format = CSVFormat
                .DEFAULT
                .withDelimiter(delimiter)
                .withQuote(quote);
        if (firstRecordAsHeader) {
            format.withFirstRecordAsHeader();
        } else {
            format.withHeader(header);
        }
        if (allowMissingColumnNames) {
            format.withAllowMissingColumnNames();
        }
        return format;
    }

}

