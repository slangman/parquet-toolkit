package kz.hustle.tools.merge;

import kz.hustle.ParquetFolder;
import kz.hustle.tools.common.InputSource;
import kz.hustle.tools.common.ThreadPool;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HiddenFileFilter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

//TODO: Перевести
//TODO: Кидать исключения при всех ошибках
//TODO: Проверять надо ли мержить
//TODO: Задавать размер выходного файла

/**
 * @author Daniil Ivantsov
 * <p>
 * Укрупняет parquet файлы в несколько потоков. Результаты работы потоков не объединяются.
 * Если в списке укрупняемых файлов содержатся файлы с различными схемами,
 * разделяет укрупненные файлы по схемам.
 * При создании экземпляра класса можно указать следующие опции:
 * - суммарный размер файлов, передаваемых в один поток для укрупнения (по умолчанию 128Mb);
 * - размер пула потоков (по умолчанию - 64);
 * - размер rowGroup в выходных файлах (по умолчанию 128Mb);
 * - тип кодека для сжатия файлов (по умолчанию Snappy, наследуется от ParquetMergerImpl);
 * - путь для сохранения выходных файлов (по умолчанию выходные файлы сохраняются в каталоге /merged,
 * созданном в каталоге с входными файлами).
 * - удаление исходных файлов (по умолчанию исходные файлы сохраняются).
 */

public class SimpleMultithreadedParquetMerger extends MultithreadedParquetMerger {

    volatile Set<String> brokenFiles = ConcurrentHashMap.newKeySet();
    private int threadPoolSize = 64;
    private int outputRowGroupSize = 128 * 1024 * 1024;
    private int inputChunkSize = 128 * 1024 * 1024;
    private String outputPath;
    private boolean removeInputFiles;
    private boolean removeInputDir;
    private int badBlockReadAttempts = 5;
    private long badBlockReadTimeout = 30000;
    private boolean keepEmptyFiles = false;
    private boolean supportInt96 = false;

    private SimpleMultithreadedParquetMerger() {
    }

    public static SimpleMultithreadedParquetMerger.Builder builder(ParquetFolder parquetFolder) {
        return new SimpleMultithreadedParquetMerger().new Builder(parquetFolder);
    }

    public class Builder {
        private Builder(ParquetFolder parquetFolder) {
            SimpleMultithreadedParquetMerger.this.inputPath = parquetFolder.getPath();
            SimpleMultithreadedParquetMerger.this.conf = parquetFolder.getConf();
        }

        private Builder(){}

        public Builder source(InputSource source) {
            SimpleMultithreadedParquetMerger.this.inputSource = source;
            return this;
        }

        /**
         * Sets the number of merger threads running simultaneously
         *
         * @param threadPoolSize int
         */
        public Builder withThreadPoolSize(int threadPoolSize) {
            SimpleMultithreadedParquetMerger.this.threadPoolSize = threadPoolSize;
            return this;
        }

        public Builder withOutputRowGroupSize(int outputRowGroupSize) {
            SimpleMultithreadedParquetMerger.this.outputRowGroupSize = outputRowGroupSize;
            return this;
        }

        public Builder withInputChunkSize(int inputChunkSize) {
            if (inputChunkSize < (1024 * 1024)) {
                System.out.println("Input chunk size can not be less than 1Mb");
                SimpleMultithreadedParquetMerger.this.inputChunkSize = 0;
                return this;
            }
            SimpleMultithreadedParquetMerger.this.inputChunkSize = inputChunkSize;
            return this;
        }

        public Builder withCompressionCodec(CompressionCodecName compressionCodecName) {
            SimpleMultithreadedParquetMerger.this.compressionCodecName = compressionCodecName;
            return this;
        }

        public Builder withOutputPath(String outputPath) {
            SimpleMultithreadedParquetMerger.this.outputPath = outputPath;
            return this;
        }

        public Builder withRemoveInputFiles() {
            SimpleMultithreadedParquetMerger.this.removeInputFiles = true;
            return this;
        }

        public Builder withRemoveInputDir() {
            SimpleMultithreadedParquetMerger.this.removeInputDir = true;
            return this;
        }

        public Builder withOutputFileName(String outputFileName) {
            SimpleMultithreadedParquetMerger.this.outputFileName = outputFileName;
            return this;
        }

        public Builder withBadBlockReadAttempts(int badBlockReadAttempts) {
            SimpleMultithreadedParquetMerger.this.badBlockReadAttempts = badBlockReadAttempts;
            return this;
        }

        public Builder withBadBlockReadTimeout(long badBlockReadTimeout) {
            SimpleMultithreadedParquetMerger.this.badBlockReadTimeout = badBlockReadTimeout;
            return this;
        }

        public Builder withInt96Fields(String... fields) {
            if (fields == null || fields.length == 0) {
                return this;
            }
            SimpleMultithreadedParquetMerger.this.conf.set("parquet.avro.readInt96AsFixed", "true");
            String int96Fields;
            if (fields.length == 1) {
                int96Fields = fields[0];
            } else {
                StringBuilder sb = new StringBuilder();
                for (String field : fields) {
                    sb.append(field).append(",");
                }
                sb.deleteCharAt(sb.length() - 1);
                int96Fields = sb.toString();
            }
            SimpleMultithreadedParquetMerger.this.conf.set("parquet.avro.writeFixedAsInt96", int96Fields);
            return this;
        }

        public Builder withInt96FieldsSupport() {
            SimpleMultithreadedParquetMerger.this.conf.set("parquet.avro.readInt96AsFixed", "true");
            SimpleMultithreadedParquetMerger.this.supportInt96 = true;
            return this;
        }

        private Builder withKeepEmptyFiles() {
            SimpleMultithreadedParquetMerger.this.keepEmptyFiles = true;
            return this;
        }

        public SimpleMultithreadedParquetMerger build() {
            return SimpleMultithreadedParquetMerger.this;
        }
    }

    public Configuration getConf() {
        return conf;
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public boolean isRemoveInputFiles() {
        return removeInputFiles;
    }

    public boolean isRemoveInputDir() {
        return removeInputDir;
    }

    public int getBadBlockReadAttempts() {
        return badBlockReadAttempts;
    }

    public long getBadBlockReadTimeout() {
        return badBlockReadTimeout;
    }

    public int getOutputRowGroupSize() {
        return outputRowGroupSize;
    }

    public int getInputChunkSize() {
        return inputChunkSize;
    }

    @Override
    public void merge() throws IOException, InterruptedException, MergingNotCompletedException {
        super.merge();
        this.fs = DistributedFileSystem.get(conf);
        if (!fs.exists(inputPath)) {
            throw new FileNotFoundException("Directory " + inputPath + " does not exist");
        }
        long start = System.currentTimeMillis();
        String dirRenamed = renameDir(inputPath.toString());
        String tempDir = dirRenamed + "/merged/";
        List<FileStatus> filesToMerge = getInputFileStatusesFromDirectory(dirRenamed);
        boolean filesMergedSuccessfully;
        if (filesToMerge != null && !filesToMerge.isEmpty()) {
            filesMergedSuccessfully = getFilesMergedSuccessfully(filesToMerge, tempDir);
        } else {
            System.out.println("No files to merge.");
            renameDir(dirRenamed, inputPath.toString());
            return;
        }
        if (!filesMergedSuccessfully) {
            renameDir(dirRenamed, inputPath.toString());
            throw new MergingNotCompletedException("Errors while merging files. Source directory would be kept.");
        }
        if (outputPath != null) {
            if (moveFiles(tempDir, outputPath)) {
                System.out.println("Files moved successfully.");
                fs.delete(new Path(tempDir), true);
            } else {
                System.out.println("Errors while moving files. Source directory would not be removed.");
            }
        } else {
            outputPath = tempDir;
        }
        boolean[] filesRemovedSuccessfully = {true};
        if (removeInputFiles) {
            System.out.println("Removing input files...");
            filesToMerge.forEach(file -> {
                try {
                    fs.delete(file.getPath(), false);
                    System.out.println(file.getPath() + " removed.");
                } catch (IOException e) {
                    e.printStackTrace();
                    filesRemovedSuccessfully[0] = false;
                }
            });
            if (filesRemovedSuccessfully[0]) {
                System.out.println("Files removed successfully.");
            } else {
                System.out.println("Errors while removing files. Some files are not removed.");
            }
        }
        if (!outputPath.equals(inputPath.toString())) {
            renameDir(dirRenamed, inputPath.toString());
            if (removeInputDir && fs.listStatus(inputPath).length == 0) {
                if (fs.delete(inputPath, true)) {
                    System.out.println("Input directory removed successfully.");
                } else {
                    System.out.println("Error while removing input directory.");
                }
            }
        } else {
            if (fs.delete(new Path(dirRenamed), true)) {
                System.out.println("Temp directory removed successfully.");
            } else {
                System.out.println("Error while removing input directory.");
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("Folder " + inputPath.toString() + " merged in " + MergeUtils.getWorkTime(end - start) + ".");
    }

    private boolean mergeFiles(List<FileStatus> inputFiles, String outputDir) throws IOException, InterruptedException {
        if (inputFiles == null || inputFiles.isEmpty()) {
            return false;
        }
        ThreadPool threadPool = new ThreadPool(threadPoolSize);
        Schema schema = getSchema(inputFiles);
        if (schema == null) {
            return false;
        }
        setInt96Fields(schema);
        long sizeCounter = 0;
        //TODO: Хз зачем, надо убрать наверное
        //Collection<List<MergedFile>> chunks = new ArrayList<>();
        List<MergedFile> chunk = new ArrayList<>();
        for (FileStatus f : inputFiles) {
            chunk.add(new MergedFile(f.getPath().toString(), f.getPath().getName(), f.getLen(), false));
            sizeCounter = sizeCounter + f.getLen();
            if (sizeCounter >= (inputChunkSize)) {
                while (threadPool.getQueueSize() > 1) {
                }
                try {
                    chunksCounterMT.getAndIncrement();
                    List<MergedFile> chunk1 = new ArrayList<>(chunk);
                    threadPool.addNewThread(new SimpleMergeThread(this,
                            chunk1,
                            schema,
                            outputDir,
                            outputFileName,
                            chunksCounterMT.get(),
                            schemaCounterMT.get(),
                            outputRowGroupSize,
                            compressionCodecName,
                            badBlockReadAttempts,
                            badBlockReadTimeout));
                    chunk.clear();
                    sizeCounter = 0;
                } catch (IOException e) {
                    e.printStackTrace();
                    return false;
                }
            }
        }
        if (!chunk.isEmpty()) {
            while (threadPool.getQueueSize() > 1) {
            }
            chunksCounterMT.getAndIncrement();
            threadPool.addNewThread(new SimpleMergeThread(this,
                    chunk,
                    schema,
                    outputDir,
                    outputFileName,
                    chunksCounterMT.get(),
                    schemaCounterMT.get(),
                    outputRowGroupSize,
                    compressionCodecName,
                    badBlockReadAttempts,
                    badBlockReadTimeout));
        }
        threadPool.shutDown();
        threadPool.await();
        if (!filesWithDifferentSchema.isEmpty()) {
            schemaCounterMT.getAndIncrement();
            chunksCounterMT.set(0);
            List<FileStatus> mergedFiles = getMergedFileStatusList(filesWithDifferentSchema);
            filesWithDifferentSchema.clear();
            mergeFiles(mergedFiles, outputDir);
        }
        return true;
    }

    protected List<FileStatus> getInputFileStatusesFromDirectory(String directory) throws IOException {
        FileStatus[] inputFiles = fs.listStatus(new Path(directory), HiddenFileFilter.INSTANCE);
        List<FileStatus> result = new ArrayList<>();
        if (inputFiles.length < 2) {
            return result;
        }
        for (FileStatus f : inputFiles) {
            if (f.isFile()) {
                Path path = f.getPath();
                String file = path.toString();
                if (fileMeetsRequirements(file)) {
                    if (!brokenFiles.contains(file) &&
                            !alreadyMerged.contains(file) &&
                            !filesWithDifferentSchema.contains(file)) {
                        //MergedFile mergedFile = new MergedFile(file, path.getName(), false);
                        result.add(f);
                    }
                } else {
                    if (file.endsWith(".parq_merger_") || file.endsWith(".parquet_merger_")) {
                        if (fs.exists(path)) {
                            fs.delete(path, false);
                        }
                    }
                }
            }
        }
        return result;
    }

    protected List<FileStatus> getMergedFileStatusList(Set<String> filesWithDifferentSchema) {
        List<FileStatus> result = new ArrayList<>();
        filesWithDifferentSchema.forEach(file -> {
            try {
                result.addAll(Arrays.asList(fs.listStatus(new Path(file))));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return result;
    }

    protected boolean moveFiles(String sourceDir, String destDir) {
        Path sourcePath = new Path(sourceDir);
        Path destPath = new Path(destDir);
        try {
            if (fs.exists(sourcePath)
                    && fs.getFileStatus(sourcePath).isDirectory()
            ) {
                if (fs.exists(destPath)) {
                    if (fs.getFileStatus(destPath).isFile()) {
                        System.out.println("Cannot move: destination path is file");
                        return false;
                    } else {

                        FileStatus[] sourceFiles = fs.listStatus(sourcePath);
                        for (FileStatus sourceFile : sourceFiles) {
                            fs.rename(sourceFile.getPath(), new Path(destPath.toString() + "/" + sourceFile.getPath().getName()));
                        }
                    }
                } else {
                    fs.mkdirs(destPath);
                    FileStatus[] sourceFiles = fs.listStatus(sourcePath);
                    for (FileStatus sourceFile : sourceFiles) {
                        fs.rename(sourceFile.getPath(), new Path(destPath.toString() + "/" + sourceFile.getPath().getName()));
                    }
                }
            } else {
                System.out.println("Source path does not exists or is not a directory");
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    boolean getFilesMergedSuccessfully(List<FileStatus> filesToMerge, String tempDir) {
        try {
            boolean filesMerged = mergeFiles(filesToMerge, tempDir);
            boolean filesMergedSuccessfully = checkTempFiles(tempDir);
            return filesMerged && filesMergedSuccessfully;
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }

    boolean mergingNeeded() {
        return false;
    }

    Schema getSchema(List<FileStatus> inputFiles) throws IOException, InterruptedException {
        AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<>();
        for (FileStatus inputFile : inputFiles) {
            ParquetReader.Builder<GenericRecord> builder = ParquetReader.builder(readSupport, inputFile.getPath());
            ParquetReader<GenericRecord> reader = builder.withConf(conf).build();
            /*ParquetReader<GenericRecord> reader = AvroParquetReader
                    .<GenericRecord>builder(HadoopInputFile.fromPath(inputFile.getPath(), conf))
                    .withConf(conf)
                    .build();*/
            int attempts = 0;
            GenericRecord record = null;
            boolean success = false;
            while (!success) {
                success = true;
                try {
                    record = reader.read();
                } catch (BlockMissingException e) {
                    if (attempts < badBlockReadAttempts) {
                        Thread.sleep(badBlockReadTimeout);
                        builder.withConf(conf).build();
                        /*reader = AvroParquetReader
                                .<GenericRecord>builder(HadoopInputFile.fromPath(inputFile.getPath(), conf))
                                .withConf(conf)
                                .build();*/
                        e.printStackTrace();
                        System.out.println("Retry reading file. Attempt " + (++attempts));
                        success = false;
                    } else {
                        throw e;
                    }
                }
            }
            if (record != null) {
                reader.close();
                return record.getSchema();
            }
            reader.close();
        }
        return null;
    }

    private void setInt96Fields(Schema schema) {
        if (supportInt96) {
            List<String> int96FieldNames = new ArrayList<>();
            schema.getFields().forEach(field -> {
                for (Schema type : field.schema().getTypes()) {
                    if (type.getName().equals("INT96")) {
                        int96FieldNames.add(field.name());
                        break;
                    }
                }
            });
            if (!int96FieldNames.isEmpty()) {
                String int96Fields;
                StringBuilder sb = new StringBuilder();
                for (String field : int96FieldNames) {
                    sb.append(field).append(",");
                }
                sb.deleteCharAt(sb.length() - 1);
                int96Fields = sb.toString();
                conf.set("parquet.avro.writeFixedAsInt96", int96Fields);
            }
        }
    }

}
