package kz.hustle.tools;

import kz.hustle.ParquetFolder;
import kz.hustle.tools.merge.MergingNotCompletedException;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GroupParquetMerger extends ParquetMergerImpl {

    private int outputRowGroupSize = 128 * 1024 * 1024;
    private int outputChunkSize = 0;
    private int schemaCounter = 1;

    private GroupParquetMerger() {
    }

    public static GroupParquetMerger.Builder builder(ParquetFolder parquetFolder) {
        return new GroupParquetMerger().new Builder(parquetFolder);
    }

    public class Builder {
        private Builder(ParquetFolder parquetFolder) {
            GroupParquetMerger.this.inputPath = parquetFolder.getPath();
            GroupParquetMerger.this.conf = parquetFolder.getConf();
        }

        public GroupParquetMerger build() {
            return GroupParquetMerger.this;
        }
    }

    @Override
    public void merge() throws IOException, InterruptedException, MergingNotCompletedException {
        super.merge();
        if (!fs.exists(inputPath)) {
            return;
        }
        long start = System.currentTimeMillis();
        String dirRenamed = renameDir(inputPath.toString());
        List<MergedFile> filesToMerge = getInputFilesFromDirectory(dirRenamed);
        if (filesToMerge != null && filesToMerge.size() > 1) {
            //TODO: Надо как-то лучше обработать этот exception
            try {
                mergeFiles(filesToMerge, dirRenamed);
            } catch (DMCErrorDeleteFile | DMCErrorRenameFile dmcErrorDeleteFile) {
                dmcErrorDeleteFile.printStackTrace();
            }
        }
        /*List<MergedFile> filesAfterMerge = getInputFilesFromDirectory(dirRenamed);
        if (filesAfterMerge.size() > 1) {
            //TODO: Надо как-то лучше обработать этот exception
            try {
                mergeFiles(filesAfterMerge, dirRenamed);
            } catch (DMCErrorDeleteFile | DMCErrorRenameFile dmcErrorDeleteFile) {
                dmcErrorDeleteFile.printStackTrace();
            }
        }*/
        renameDir(dirRenamed, inputPath.toString());
        long end = System.currentTimeMillis();
        System.out.println("Folder merged in " + MergeUtils.getWorkTime(end - start));
        System.out.println("Different schemas in folder: " + schemaCounter);
        System.out.println("Broken files: " + brokenFiles.size());
    }

    private void mergeFiles(List<MergedFile> files, String outputDir) throws IOException, DMCErrorDeleteFile, DMCErrorRenameFile {
        if (files == null || files.isEmpty()) {
            return;
        }
        String firstFilePath = files.get(0).getPath();
        ParquetMetadata readFooter;
        try {
            readFooter = ParquetFileReader.readFooter(conf, new Path(firstFilePath), ParquetMetadataConverter.NO_FILTER);
        } catch (RuntimeException e) {
            e.printStackTrace();
            return;
        }
        FileMetaData fileMetadata = readFooter.getFileMetaData();
        String mergedFileName = (outputDir + "/"
                + outputFileName
                + (schemaCounter > 1 ? ("-schema-" + schemaCounter) : "")
                + "-part-" + chunksCounter
                + ".parq_merger_");
        ParquetFileWriter fileWriter = new ParquetFileWriter(conf, fileMetadata.getSchema(), new Path(mergedFileName), ParquetFileWriter.Mode.CREATE, 128 * 1024 * 1024, 1024);
        fileWriter.start();
        List<InputFile> inputFiles = new ArrayList<>();
        files.forEach(file -> {
            try {
                inputFiles.add(HadoopInputFile.fromPath(new Path(file.getPath()), conf));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        //fileWriter.mergeRowGroups(inputFiles, 128 * 1024 * 1024, false, CompressionCodecName.GZIP);
        fileWriter.end(fileMetadata.getKeyValueMetaData());

    }
}
