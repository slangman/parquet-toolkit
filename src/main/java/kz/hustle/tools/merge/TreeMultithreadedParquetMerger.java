package kz.hustle.tools.merge;

import kz.hustle.ParquetFolder;
import kz.hustle.tools.common.ThreadPool;
import kz.hustle.tools.merge.exception.MergingNotCompletedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.hadoop.util.HiddenFileFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

//TODO: Неадекватно работает если в папке файлы с разными схемами. Подумать что можно сделать.

/**
 * @author Daniil Ivantsov
 * <p>
 * Укрупняет parquet файлы в несколько потоков.
 * Все файлы с одинаковой схемой на выходе объединяются в один файл.
 * При создании экземпляра класса можно указать следующие опции:
 * - размер пула потоков (по умолчанию - 64);
 * - количество файлов, передаваемых в один поток (по умолчанию - 5);
 * - имя выходного файла. Если в укрупняемом каталоге несколько схем, то к имени файла добавляется "-schemaN"
 * - путь для сохранения выходных файлов (по умолчанию выходные файлы сохраняются в каталоге /merged,
 * созданном в каталоге с входными файлами).
 */

public class TreeMultithreadedParquetMerger extends MultithreadedParquetMerger {
    private static final Logger logger = LogManager.getLogger(TreeMultithreadedParquetMerger.class);
    private Configuration conf;
    //private FileSystem fs;
    private int threadPoolSize = 64;
    private int threadChunkSize = 5;
    private Path inputPath;
    private String outputFileName;
    private Path outputPath;
    private boolean removeBrokenFiles;
    private boolean removeInputFiles;
    private boolean removeInputDir;
    private long outputChunkSize = -1;

    private TreeMultithreadedParquetMerger() {
    }

    public static Builder builder(ParquetFolder parquetFolder) {
        return new TreeMultithreadedParquetMerger().new Builder(parquetFolder);
    }

    public class Builder {

        private Builder(ParquetFolder parquetFolder) {
            TreeMultithreadedParquetMerger.this.inputPath = parquetFolder.getPath();
            TreeMultithreadedParquetMerger.this.conf = parquetFolder.getConf();
        }

        public Builder withThreadPoolSize(int threadPoolSize) {
            TreeMultithreadedParquetMerger.this.threadPoolSize = threadPoolSize;
            return this;
        }

        public Builder withThreadChunkSize(int threadChunkSize) {
            TreeMultithreadedParquetMerger.this.threadChunkSize = threadChunkSize;
            return this;
        }

        public Builder withOutputFileName(String outputFileName) {
            TreeMultithreadedParquetMerger.this.outputFileName = outputFileName;
            return this;
        }

        public Builder withRemoveBrokenFiles(boolean removeBrokenFiles) {
            TreeMultithreadedParquetMerger.this.removeBrokenFiles = removeBrokenFiles;
            return this;
        }

        /**
         * Determines the output path of output files. If not used the output files are stored in the same folder
         * as the input files.
         *
         * @param outputPath
         * @return
         */
        public Builder withOutputPath(Path outputPath) {
            TreeMultithreadedParquetMerger.this.outputPath = outputPath;
            return this;
        }

        /**
         * If this enabled, the input files would be deleted after successful merging
         *
         * @return
         */
        public Builder withRemoveInputFiles() {
            TreeMultithreadedParquetMerger.this.removeInputFiles = true;
            return this;
        }

        public Builder withRemoveInputDir() {
            TreeMultithreadedParquetMerger.this.removeInputDir = true;
            return this;
        }

        public Builder withOutputChunkSize(long outputChunkSize) {
            TreeMultithreadedParquetMerger.this.outputChunkSize = outputChunkSize;
            return this;
        }

        public TreeMultithreadedParquetMerger build() {
            return TreeMultithreadedParquetMerger.this;
        }

    }

    public Configuration getConf() {
        return conf;
    }

    public FileSystem getFs() {
        return fs;
    }

    @Override
    public void merge() throws IOException, InterruptedException, MergingNotCompletedException {
        super.merge();
        if (conf == null) {
            logger.error("No configuration provided");
            return;
        }
        this.fs = DistributedFileSystem.get(conf);
        if (fs == null) {
            logger.error("No file system provided");
            return;
        }
        if (!fs.exists(inputPath)) {
            System.out.println("Directory not found");
            return;
        }
        long start = System.currentTimeMillis();
        String dirRenamed = renameDir(inputPath.toString());
        String tempDir = dirRenamed + "/merged/";
        List<MergedFile> filesToMerge = getInputFilesFromDirectory(dirRenamed);
        boolean mergedSuccessfully;
        if (filesToMerge != null && !filesToMerge.isEmpty() && filesToMerge.size() > 1) {
            mergedSuccessfully = getFilesMergedSuccessfully(filesToMerge, tempDir);
        } else {
            System.out.println("No files to merge");
            renameDir(dirRenamed, inputPath.toString());
            return;
        }
        if (!mergedSuccessfully) {
            renameDir(dirRenamed, inputPath.toString());
            throw new MergingNotCompletedException("Errors while merging files. Source directory would be kept.");
        }
        if (removeBrokenFiles) {
            removeBrokenFiles();
        }
        if (outputPath != null) {
            if (moveFile((fs.listStatus(new Path(tempDir))[0]).getPath(), outputPath)) {
                System.out.println("Files moved successfully.");
                fs.delete(new Path(tempDir), true);
            }
        }
        boolean[] filesRemovedSuccessfully = {true};
        if (removeInputFiles) {
            System.out.println("Removing input files...");
            filesToMerge.forEach(file -> {
                try {
                    fs.delete(new Path(file.getPath()), false);
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


    /*private void setDefaultParams() {
        this.threadPoolSize = 64;
        this.chunkSize = 5;
    }*/

    public List<MergedFile> getInputFilesFromDirectory(String directory) throws IOException {
        FileStatus[] inputFiles = fs.listStatus(new Path(directory), HiddenFileFilter.INSTANCE);
        List<MergedFile> result = new ArrayList<>();
        /*if (inputFiles.length < 2) {
            return result;
        }*/
        logger.info("Files in folder: " + inputFiles.length);
        for (FileStatus f : inputFiles) {
            if (f.isFile()) {
                Path path = f.getPath();
                String file = path.toString();
                if (fileMeetsRequirements(file)) {
                    if (!brokenFilesMT.contains(file)) {
                        MergedFile mergedFile = new MergedFile(file, path.getName(), false);
                        result.add(mergedFile);
                    }
                } else {
                    if (file.endsWith(".parq_merger_")) {
                        if (fs.exists(path)) {
                            fs.delete(path, false);
                        }
                    }
                }
            }
        }
        return result;
    }

    boolean getFilesMergedSuccessfully(List<MergedFile> filesToMerge, String tempDir) {
        try {
            boolean filesMerged = mergeFiles(filesToMerge, tempDir, false);
            boolean filesMergedSuccessfully = checkTempFiles(tempDir);
            return filesMerged && filesMergedSuccessfully;
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }

    //Переменная removeInputFiles нужна чтобы на первой итерации НЕ УДАЛЯТЬ исходные файлы.
    private boolean mergeFiles(List<MergedFile> inputFiles, String outputDir, boolean removeInputFiles) throws IOException, InterruptedException {
        AtomicInteger counter = new AtomicInteger();
        Collection<List<MergedFile>> chunks = inputFiles
                .stream()
                .collect(Collectors.groupingBy(e -> counter.getAndIncrement() / threadChunkSize))
                .values();
        ThreadPool threadPool = new ThreadPool(threadPoolSize);
        chunks.forEach(chunk -> {
            while (threadPool.getQueueSize() > 1) {
            }
            threadPool.addNewThread(new TreeMergeThread(this, chunk, outputDir, removeInputFiles));
        });
        threadPool.shutDown();
        threadPool.await();
        List<MergedFile> filesAfterMerge = null;
        filesAfterMerge = getInputFilesFromDirectory(outputDir);

        if (filesAfterMerge.size() > 1) {
            mergeFiles(filesAfterMerge, outputDir, true);
        } else {
            return true;
        }
        return true;
    }

    private boolean moveFile(Path inputPath, Path outputPath) throws IOException {
        if (!fs.getFileStatus(inputPath).isFile()) {
            System.out.println(inputPath.toString() + " is not a file");
            return false;
        }
        if (outputPath.toString().startsWith(conf.get("fs.defaultFS"))) {
            outputPath = new Path(outputPath.toString().substring(conf.get("fs.defaultFS").length()));
        }
        if (fs.exists(outputPath)) {
            if (fs.getFileStatus(outputPath).isFile()) {
                System.out.println("File already exists: " + outputPath);
                throw new IOException();
            } else {
                Path finalOutputPath;
                if (outputPath.toString().endsWith("/")) {
                    finalOutputPath = new Path(outputPath + inputPath.getName());
                } else {
                    finalOutputPath = new Path(outputPath + "/" + inputPath.getName());
                }
                fs.rename(inputPath, finalOutputPath);
            }
        } else {
            if (outputPath.toString().endsWith("/")) {
                System.out.println("No such file or directory: " + outputPath.toString());
                return false;
            } else {
                Path outputDir = new Path(outputPath.toString().substring(0, outputPath.toString().lastIndexOf("/")));
                if (!fs.exists(outputDir)) {
                    fs.mkdirs(outputDir);
                }
                fs.rename(inputPath, outputPath);
            }
        }
        System.out.println(inputPath.toString() + " moved to " + outputPath);
        return true;
    }

    /*private boolean fileMeetsRequirements(String file) {
        return ((file.endsWith(".parquet") || (file.matches(".*" + "sequence-" + ".*") && file.endsWith(".parq")))
                && !file.matches(".*" + "_temporary/0" + ".*"));
    }


    private String renameDir(String path) {
        String newPath = "";
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        String dirName = path.substring(path.lastIndexOf("/") + 1);
        String dirName1 = "_" + dirName;
        newPath = path.substring(0, path.lastIndexOf("/") + 1) + dirName1;
        try {
            fs.rename((new Path(path)), new Path(newPath));
            return newPath;
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }

    private String renameDir(String oldPath, String newPath) {
        try {
            fs.rename((new Path(oldPath)), new Path(newPath));
            return newPath;
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }

    private void removeBrokenFiles() {
        if (brokenFiles.isEmpty()) {
            return;
        }
        System.out.println("Broken files:");
        brokenFiles.forEach(System.out::println);
        System.out.println("Removing broken files...");
        brokenFiles.forEach(file -> {
            try {
                fs.delete(new Path(file), false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }*/


}
