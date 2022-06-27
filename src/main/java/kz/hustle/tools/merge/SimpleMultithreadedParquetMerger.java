package kz.hustle.tools.merge;

import kz.hustle.ParquetFolder;
import kz.hustle.tools.common.InputFiles;
import kz.hustle.tools.common.InputPath;
import kz.hustle.tools.common.InputSource;
import kz.hustle.tools.common.ThreadPool;
import kz.hustle.tools.merge.exception.MergingNotCompletedException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

//TODO: Output file size

/**
 * @author Daniil Ivantsov
 * <p>
 * Megres .parquet files in multiple-thread approach. Every thread creates a single file.
 * Input files with different schemas are merged separately.
 */

public class SimpleMultithreadedParquetMerger extends MultithreadedParquetMerger {

    volatile Set<String> brokenFiles = ConcurrentHashMap.newKeySet();

    private int outputRowGroupSize = 128 * 1024 * 1024;
    private int inputChunkSize = 128 * 1024 * 1024;
    private String outputPath;
    private boolean removeInputFiles;
    private boolean removeInputDir;
    private boolean supportInt96 = false;

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleMultithreadedParquetMerger.class);

    private SimpleMultithreadedParquetMerger() {
    }

    /**
     * @param parquetFolder
     * @deprecated will be removed in 1.0.0
     * use {@link #builder(Configuration)} instead.
     */
    @Deprecated
    public static SimpleMultithreadedParquetMerger.DeprecatedBuilder builder(ParquetFolder parquetFolder) {
        return new SimpleMultithreadedParquetMerger().new DeprecatedBuilder(parquetFolder);
    }

    public static SimpleMultithreadedParquetMerger.Builder builder(Configuration conf) {
        return new SimpleMultithreadedParquetMerger().new Builder(conf);
    }

    public class Builder {
        private Builder(Configuration conf) {
            SimpleMultithreadedParquetMerger.this.conf = conf;
            try {
                SimpleMultithreadedParquetMerger.this.fs = DistributedFileSystem.get(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private Builder() {
        }

        public Builder inputSource(InputSource source) {
            SimpleMultithreadedParquetMerger.this.inputSource = source;
            return this;
        }

        public Builder threadPoolSize(int threadPoolSize) {
            SimpleMultithreadedParquetMerger.this.threadPoolSize = threadPoolSize;
            return this;
        }

        public Builder outputRowGroupSize(int outputRowGroupSize) {
            SimpleMultithreadedParquetMerger.this.outputRowGroupSize = outputRowGroupSize;
            return this;
        }

        public Builder outputChunkSizeMegabytes(int outputChunkSizeMegabytes) {
            if (outputChunkSize < 1) {
                LOGGER.warn("Output chunk size can not be less than 1 MB. The output chunk size is set to default (128 MB)");
                return this;
            }
            SimpleMultithreadedParquetMerger.this.outputChunkSize = outputChunkSizeMegabytes * 1024 * 1024;
            return this;
        }

        public Builder compressionCodec(CompressionCodecName compressionCodecName) {
            SimpleMultithreadedParquetMerger.this.compressionCodecName = compressionCodecName;
            return this;
        }

        /*
        If outputPath value is linking to a folder, the output files are stored in that folder after merging
        with default names ('merged-datafile-part-0.parquet', 'merged-datafile-part-1.parquet' etc.).
        If output path contains file name, e.g. 'custom-name.parquet', then output file names would be the same
        with addition of part number, e.g.: 'custom-name-part-0.parquet'.
        If output path is a file name with extension other than '.parquet' or '.parq', then the path would be recognized
        as a folder path.
         */
        public Builder outputPath(String outputPath) {
            SimpleMultithreadedParquetMerger.this.outputPath = outputPath;
            return this;
        }

        public Builder removeInputAfterMerging(boolean removeInput, boolean moveToTrash) {
            SimpleMultithreadedParquetMerger.this.removeInput = removeInput;
            SimpleMultithreadedParquetMerger.this.moveToTrash = moveToTrash;
            return this;
        }

        public Builder badBlockReadAttempts(int badBlockReadAttempts) {
            SimpleMultithreadedParquetMerger.this.badBlockReadAttempts = badBlockReadAttempts;
            return this;
        }

        public Builder badBlockReadTimeout(long badBlockReadTimeout) {
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

        public SimpleMultithreadedParquetMerger build() {
            return SimpleMultithreadedParquetMerger.this;
        }
    }

    /**
     * @deprecated will be removed in 1.0.0
     */
    @Deprecated
    public class DeprecatedBuilder {
        private DeprecatedBuilder(ParquetFolder parquetFolder) {
            SimpleMultithreadedParquetMerger.this.inputPath = parquetFolder.getPath();
            SimpleMultithreadedParquetMerger.this.conf = parquetFolder.getConf();
        }

        private DeprecatedBuilder() {
        }

        /**
         * Sets the number of merger threads running simultaneously.
         *
         * @param threadPoolSize a number of threads
         * @deprecated will be removed in 1.0.0
         */
        @Deprecated
        public DeprecatedBuilder withThreadPoolSize(int threadPoolSize) {
            SimpleMultithreadedParquetMerger.this.threadPoolSize = threadPoolSize;
            return this;
        }

        /**
         * Sets the size of the row group in output file.
         *
         * @param outputRowGroupSize the size of the row group in output file
         * @deprecated will be removed in 1.0.0
         */
        @Deprecated
        public DeprecatedBuilder withOutputRowGroupSize(int outputRowGroupSize) {
            SimpleMultithreadedParquetMerger.this.outputRowGroupSize = outputRowGroupSize;
            return this;
        }

        /**
         * @param inputChunkSize
         * @deprecated will be removed in 1.0.0
         */
        @Deprecated
        public DeprecatedBuilder withInputChunkSize(int inputChunkSize) {
            if (inputChunkSize < (1024 * 1024)) {
                System.out.println("Input chunk size can not be less than 1MB");
                SimpleMultithreadedParquetMerger.this.inputChunkSize = 0;
                return this;
            }
            SimpleMultithreadedParquetMerger.this.inputChunkSize = inputChunkSize;
            return this;
        }

        /**
         * @param compressionCodecName
         * @deprecated will be removed in 1.0.0
         */
        @Deprecated
        public DeprecatedBuilder withCompressionCodec(CompressionCodecName compressionCodecName) {
            SimpleMultithreadedParquetMerger.this.compressionCodecName = compressionCodecName;
            return this;
        }

        /**
         * @param outputPath
         * @rdeprecated will be removed in 1.0.0
         */
        @Deprecated
        public DeprecatedBuilder withOutputPath(String outputPath) {
            SimpleMultithreadedParquetMerger.this.outputPath = outputPath;
            return this;
        }

        @Deprecated
        public DeprecatedBuilder withRemoveInputFiles() {
            SimpleMultithreadedParquetMerger.this.removeInputFiles = true;
            return this;
        }

        @Deprecated
        public DeprecatedBuilder withRemoveInputDir() {
            SimpleMultithreadedParquetMerger.this.removeInputDir = true;
            return this;
        }

        @Deprecated
        public DeprecatedBuilder withOutputFileName(String outputFileName) {
            SimpleMultithreadedParquetMerger.this.outputFileName = outputFileName;
            return this;
        }

        @Deprecated
        public DeprecatedBuilder withBadBlockReadAttempts(int badBlockReadAttempts) {
            SimpleMultithreadedParquetMerger.this.badBlockReadAttempts = badBlockReadAttempts;
            return this;
        }

        @Deprecated
        public DeprecatedBuilder withBadBlockReadTimeout(long badBlockReadTimeout) {
            SimpleMultithreadedParquetMerger.this.badBlockReadTimeout = badBlockReadTimeout;
            return this;
        }

        @Deprecated
        public DeprecatedBuilder withInt96Fields(String... fields) {
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

        @Deprecated
        public DeprecatedBuilder withInt96FieldsSupport() {
            SimpleMultithreadedParquetMerger.this.conf.set("parquet.avro.readInt96AsFixed", "true");
            SimpleMultithreadedParquetMerger.this.supportInt96 = true;
            return this;
        }

        public SimpleMultithreadedParquetMerger build() {
            return SimpleMultithreadedParquetMerger.this;
        }
    }

    public String getOutputPath() {
        return outputPath;
    }

    @Deprecated
    public boolean isRemoveInputFiles() {
        return removeInputFiles;
    }

    @Deprecated
    public boolean isRemoveInputDir() {
        return removeInputDir;
    }

    public int getOutputRowGroupSize() {
        return outputRowGroupSize;
    }

    @Deprecated
    public int getInputChunkSize() {
        return inputChunkSize;
    }

    @Override
    public void merge() throws MergingNotCompletedException, IOException, InterruptedException {
        if (inputSource != null) {
            newMerge();
        } else {
            oldMerge();
        }
    }

    public void newMerge() throws IOException {
        if (inputSource instanceof InputFiles && outputPath == null) {
            throw new IllegalArgumentException("Output path can not be null when input source is a list of paths.");
        }
        String tempDir = conf.get("hadoop.tmp.dir") + "/parquet-merger/" + UUID.randomUUID();
        fs.mkdirs(new Path(tempDir));
        boolean mergeIsSuccessful = getFilesMergedSuccessfully(inputSource.getFiles(conf), tempDir);
        if (mergeIsSuccessful) {
            moveFilesFromTempDir(tempDir);
            fs.delete(new Path(tempDir), true);
            if (removeInput) {
                removeInputFiles();
            }
        }
    }

    private void moveFilesFromTempDir(String tempDir) throws IOException {
        if (outputPath == null) {
            setOutputPath();
        }
        Path outputPathFs = new Path(outputPath);
        if (fs.exists(outputPathFs)) {
            if (fs.getFileStatus(outputPathFs).isDirectory()) {
                if (fs.listStatus(outputPathFs).length > 0) {
                    throw new IOException("Output directory must be empty: " + outputPath
                            + System.lineSeparator()
                            + "Merged files are stored in temp directory: " + tempDir);
                }
                moveFilesToDir(new Path(tempDir), outputPathFs);
            } else {
                throw new IOException("File already exists: " + outputPath
                        + System.lineSeparator()
                        + "Merged files are stored in temp directory: " + tempDir);
            }
        } else {
            if (!(outputPath.endsWith(".parq") || outputPath.endsWith(".parquet"))) {
                fs.mkdirs(outputPathFs);
                moveFilesToDir(new Path(tempDir), outputPathFs);
            } else {
                moveFilesToFiles(new Path(tempDir));
            }
        }
    }

    private void moveFilesToDir(Path tempPathFs, Path outputPathFs) throws IOException {
        for (FileStatus fileStatus : fs.listStatus(tempPathFs)) {
            fs.rename(fileStatus.getPath(), new Path(outputPathFs.toString() + "/" + fileStatus.getPath().getName()));
        }
        LOGGER.info("Output files moved to " + outputPath);
    }

    private void moveFilesToFiles(Path tempPathFs) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(tempPathFs);
        Path outputDir = new Path(outputPath.substring(0, outputPath.lastIndexOf("/")));
        if (!fs.exists(outputDir)) {
            fs.mkdirs(outputDir);
        }
        for (int i = 0; i < fileStatuses.length; i++) {
            String outputPathString = outputPath.substring(0, outputPath.lastIndexOf("."))
                    + "-part-" + i
                    + outputPath.substring(outputPath.lastIndexOf("."));
            fs.rename(fileStatuses[i].getPath(), new Path(outputPathString));
        }
        LOGGER.info("Output files moved to " + outputPath);
    }

    private void setOutputPath() {
        if (inputSource instanceof InputPath) {
            String inputPath = ((InputPath) inputSource).getPath();
            outputPath = inputPath + (inputPath.endsWith("/") ? "" : "/") + "merged/";
        }
    }

    private void removeInputFiles() throws IOException {
        boolean[] filesRemovedSuccessfully = new boolean[]{true};
        LOGGER.info("Removing input files...");
        List<FileStatus> filesToDelete = inputSource.getFiles(conf);
        if (moveToTrash) {
            Trash trashTmp = new Trash(fs, conf);
            filesToDelete.forEach(file -> {
                try {
                    trashTmp.moveToTrash(file.getPath());
                    LOGGER.info(file.getPath() + " moved to trash.");
                } catch (IOException e) {
                    e.printStackTrace();
                    filesRemovedSuccessfully[0] = false;
                }
            });
        } else {
            filesToDelete.forEach(file -> {
                try {
                    fs.delete(file.getPath(), false);
                    LOGGER.info(file.getPath() + " removed.");
                } catch (IOException e) {
                    e.printStackTrace();
                    filesRemovedSuccessfully[0] = false;
                }
            });
        }
        if (filesRemovedSuccessfully[0]) {
            LOGGER.info("Files removed successfully.");
        } else {
            LOGGER.warn("Errors while removing input files. Some files are not removed.");
        }
    }


    public void oldMerge() throws IOException, InterruptedException, MergingNotCompletedException {
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
            LOGGER.warn("No files to merge.");
            renameDir(dirRenamed, inputPath.toString());
            return;
        }
        if (!filesMergedSuccessfully) {
            renameDir(dirRenamed, inputPath.toString());
            throw new MergingNotCompletedException("Errors while merging files. Source directory would be kept.");
        }
        if (outputPath != null) {
            if (moveFiles(tempDir, outputPath)) {
                LOGGER.info("Files moved successfully.");
                fs.delete(new Path(tempDir), true);
            } else {
                LOGGER.warn("Errors while moving files. Source directory would not be removed.");
            }
        } else {
            outputPath = tempDir;
        }
        boolean[] filesRemovedSuccessfully = {true};
        if (removeInputFiles) {
            LOGGER.info("Removing input files...");
            filesToMerge.forEach(file -> {
                try {
                    fs.delete(file.getPath(), false);
                    LOGGER.info(file.getPath() + " removed.");
                } catch (IOException e) {
                    e.printStackTrace();
                    filesRemovedSuccessfully[0] = false;
                }
            });
            if (filesRemovedSuccessfully[0]) {
                LOGGER.info("Files removed successfully.");
            } else {
                LOGGER.warn("Errors while removing files. Some files are not removed.");
            }
        }
        if (!outputPath.equals(inputPath.toString())) {
            renameDir(dirRenamed, inputPath.toString());
            if (removeInputDir && fs.listStatus(inputPath).length == 0) {
                if (fs.delete(inputPath, true)) {
                    LOGGER.info("Input directory removed successfully.");
                } else {
                    LOGGER.warn("Error while removing input directory.");
                }
            }
        } else {
            if (fs.delete(new Path(dirRenamed), true)) {
                LOGGER.info("Temp directory removed successfully.");
            } else {
                LOGGER.warn("Error while removing input directory.");
            }
        }
        long end = System.currentTimeMillis();
        LOGGER.info("Folder " + inputPath.toString() + " merged in " + MergeUtils.getWorkTime(end - start) + ".");
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
                        LOGGER.warn("Cannot move: destination path is file");
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
                LOGGER.warn("Source path does not exists or is not a directory");
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
                        LOGGER.info("Retry reading file. Attempt " + (++attempts));
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
