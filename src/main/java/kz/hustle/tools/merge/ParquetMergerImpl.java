package kz.hustle.tools.merge;

import kz.hustle.tools.common.InputSource;
import kz.hustle.tools.merge.exception.MergingNotCompletedException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HiddenFileFilter;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ParquetMergerImpl implements ParquetMerger {

    protected Path inputPath;
    protected InputSource inputSource;
    protected Configuration conf;
    protected FileSystem fs;
    volatile Set<String> brokenFiles = ConcurrentHashMap.newKeySet();
    volatile Set<String> filesWithDifferentSchema = ConcurrentHashMap.newKeySet();
    volatile Set<String> alreadyMerged = ConcurrentHashMap.newKeySet();
    protected String outputFileName = "merged-datafile";
    protected CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;
    protected int chunksCounter = 1;

    public CompressionCodecName getCompressionCodecName() {
        return compressionCodecName;
    }

    @Override
    public void merge() throws IOException, InterruptedException, MergingNotCompletedException {
        /*if (conf != null) {
            fs = DistributedFileSystem.get(conf);
        }*/
    }

    ParquetWriter<GenericRecord> createParquetFile(String filePath, Schema schema, int rowGroupSize, CompressionCodecName compressionCodecName) throws IOException {
        /*AvroWriteSupport<GenericRecord> writeSupport = new AvroWriteSupport<>();*/

        //return new AvroParquetWriter(new Path(filePath), schema, compressionCodecName, rowGroupSize, rowGroupSize);

        return AvroParquetWriter.<GenericRecord>builder(new Path(filePath))
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(compressionCodecName)
                .withRowGroupSize(rowGroupSize)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
    }

    protected List<MergedFile> getInputFilesFromDirectory(String directory) throws IOException {
        FileStatus[] inputFiles = fs.listStatus(new Path(directory), HiddenFileFilter.INSTANCE);
        List<MergedFile> result = new ArrayList<>();
        if (inputFiles.length < 2) {
            return result;
        }
        System.out.println("Files in folder: " + inputFiles.length);
        for (FileStatus f : inputFiles) {
            if (f.isFile()) {
                Path path = f.getPath();
                String file = path.toString();
                if (fileMeetsRequirements(file)) {
                    if (!brokenFiles.contains(file) &&
                            !alreadyMerged.contains(file) &&
                            !filesWithDifferentSchema.contains(file)) {
                        MergedFile mergedFile = new MergedFile(file, path.getName(), false);
                        result.add(mergedFile);
                    }
                } else {
                    if (file.endsWith(".parq_merger_")  || file.endsWith(".parquet_merger_")) {
                        if (fs.exists(path)) {
                            fs.delete(path, false);
                        }
                    }
                }
            }
        }
        return result;
    }

    protected String renameDir(String path) throws IOException {
        String newPath = "";
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        String dirName = path.substring(path.lastIndexOf("/") + 1);
        String dirName1 = "_" + dirName;
        newPath = path.substring(0, path.lastIndexOf("/") + 1) + dirName1;
        fs.rename((new Path(path)), new Path(newPath));
        return newPath;
    }

    protected String renameDir(String oldPath, String newPath) {
        try {
            fs.rename((new Path(oldPath)), new Path(newPath));
            return newPath;
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }

    protected void removeBrokenFiles() {
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
    }

    protected boolean fileMeetsRequirements(String file) {
        return ((file.endsWith(".parquet") || (file.endsWith(".parq")))
                && !file.matches(".*" + "_temporary/0" + ".*"));
    }

    protected List<MergedFile> createMergedFileList(Set<String> filesWithDifferentSchema) {
        List<MergedFile> result = new ArrayList<>();
        filesWithDifferentSchema.forEach(file -> {
            MergedFile mergedFile = new MergedFile();
            mergedFile.setPath(file);
            mergedFile.setFileName((new Path(file)).getName());
            mergedFile.setMustBeDeleted(false);
            result.add(mergedFile);
        });
        return result;
    }

    protected boolean checkTempFiles(String tempDir) throws IOException {
        FileStatus[] tempFiles = fs.listStatus(new Path(tempDir));
        for (FileStatus tempFile : tempFiles) {
            if (tempFile.getPath().getName().endsWith("_merger_")) {
                return false;
            }
        }
        return true;
    }
}
