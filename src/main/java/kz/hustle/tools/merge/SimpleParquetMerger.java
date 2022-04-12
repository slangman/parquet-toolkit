package kz.hustle.tools.merge;

import kz.hustle.ParquetFolder;
import kz.hustle.tools.exception.DMCErrorDeleteFile;
import kz.hustle.tools.exception.DMCErrorRenameFile;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

public class SimpleParquetMerger extends ParquetMergerImpl {

    private int outputRowGroupSize = 128 * 1024 * 1024;
    private int outputChunkSize = 0;
    private boolean removeBrokenFiles = false;
    private int schemaCounter = 1;


    private SimpleParquetMerger() {
    }

    public static Builder builder(ParquetFolder parquetFolder) {
        return new SimpleParquetMerger().new Builder(parquetFolder);
    }

    public class Builder {
        private Builder(ParquetFolder parquetFolder) {
            SimpleParquetMerger.this.inputPath = parquetFolder.getPath();
            SimpleParquetMerger.this.conf = parquetFolder.getConf();
        }

        public Builder withOutputRowGroupSize(int outputRowGroupSize) {
            SimpleParquetMerger.this.outputRowGroupSize = outputRowGroupSize;
            return this;
        }

        public Builder withOutputChunkSize(int outputChunkSize) {
            if (outputChunkSize < (1024*1024)) {
                System.out.println("Output chunk size can not be less than 1Mb");
                SimpleParquetMerger.this.outputChunkSize = 0;
                return this;
            }
            SimpleParquetMerger.this.outputChunkSize = outputChunkSize;
            return this;
        }

        public Builder withCompressionCodec(CompressionCodecName compressionCodecName) {
            SimpleParquetMerger.this.compressionCodecName = compressionCodecName;
            return this;
        }

        public SimpleParquetMerger build() {
            return SimpleParquetMerger.this;
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
        if (removeBrokenFiles) {
            removeBrokenFiles();
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
        System.out.println("Folder merged in " + MergeUtils.getWorkTime(end-start));
        System.out.println("Different schemas in folder: " + schemaCounter);
        System.out.println("Broken files: " + brokenFiles.size());
    }

    private void mergeFiles(List<MergedFile> files, String outputDir) throws IOException, DMCErrorDeleteFile, DMCErrorRenameFile {
        if (files == null || files.isEmpty()) {
            return;
        }
        StringBuilder errorStr = new StringBuilder();
        Schema schema = null;
        String firstFilePath = files.get(0).getPath();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(HadoopInputFile.fromPath(new Path(firstFilePath), conf))
                .withConf(conf)
                .build();) {
            GenericRecord record = reader.read();
            schema = record.getSchema();
        } catch (IOException e) {
            brokenFiles.add(firstFilePath);
        }
        if (schema != null) {
            String mergedFileName = (outputDir + "/"
                    + outputFileName
                    + (schemaCounter > 1 ? ("-schema-" + schemaCounter) : "")
                    + "-part-" + chunksCounter
                    + ".parq_merger_");
            ParquetWriter<GenericRecord> writer = createParquetFile(mergedFileName, schema, outputRowGroupSize, compressionCodecName);
            for (MergedFile file : files) {
                try (ParquetReader<GenericRecord> reader = AvroParquetReader
                        .<GenericRecord>builder(HadoopInputFile.fromPath(new Path(file.getPath()), conf))
                        .withConf(conf)
                        .build();) {
                    GenericRecord record = reader.read();
                    Schema schema1 = record.getSchema();
                    if (schema1.equals(schema)) {
                        while (record != null) {
                            writer.write(record);
                            long recordsSizeMB = (long) writer.getDataSize() / 1024 / 1024;
                            record = reader.read();
                            if (outputChunkSize > 0) {
                                if (recordsSizeMB >= ((outputChunkSize / 1024 / 1024) - 5) && record != null) {
                                    System.out.println(writer.getDataSize());
                                    System.out.println("File: " + outputFileName + (schemaCounter > 1 ? ("-schema-" + schemaCounter) : "") + "-part-" + chunksCounter + ".parq - " + recordsSizeMB + "MB");
                                    writer.close();
                                    try {
                                        alreadyMerged.add(mergedFileName.replace("_merger_", ""));
                                        fs.rename(new Path(mergedFileName), new Path(mergedFileName.replace("_merger_", "")));
                                    } catch (Exception e) {
                                        StringWriter errorWriter = new StringWriter();
                                        e.printStackTrace(new PrintWriter(errorWriter));
                                        errorStr.append("RENAMING ERROR: " + mergedFileName + "\n");
                                        errorStr.append("RENAMING ERROR: " + mergedFileName.replace("_merger_", "") + "\n");
                                        errorStr.append(errorWriter.toString());
                                        throw new DMCErrorRenameFile(errorStr.toString());
                                    }
                                    chunksCounter++;
                                    mergedFileName = (outputDir + "/"
                                            + outputFileName
                                            + (schemaCounter > 1 ? ("-schema-" + schemaCounter) : "")
                                            + "-part-" + chunksCounter
                                            + ".parq_merger_");
                                    writer = createParquetFile(mergedFileName, schema, outputRowGroupSize, compressionCodecName);
                                }
                            }
                        }
                        file.setMustBeDeleted(true);
                    } else {
                        filesWithDifferentSchema.add(file.getPath());
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            writer.close();
            try {
                alreadyMerged.add(mergedFileName.replace("_merger_", ""));
                fs.rename(new Path(mergedFileName), new Path(mergedFileName.replace("_merger_", "")));
            } catch (Exception e) {
                StringWriter errorWriter = new StringWriter();
                e.printStackTrace(new PrintWriter(errorWriter));
                errorStr.append("RENAMING ERROR: " + mergedFileName + "\n");
                errorStr.append("RENAMING ERROR: " + mergedFileName.replace("_merger_", "") + "\n");
                errorStr.append(errorWriter.toString());
                throw new DMCErrorRenameFile(errorStr.toString());
            }
        }
        for (MergedFile file : files) {
            try {
                if (file.mustBeDeleted()) {
                    fs.delete(new Path(file.getPath()), false);
                }
            } catch (Exception e) {
                System.out.println("DELETING ERROR: " + e.getLocalizedMessage());
                StringWriter errorWriter = new StringWriter();
                e.printStackTrace(new PrintWriter(errorWriter));
                errorStr.append("DELETING ERROR: " + file.getPath() + "\n");
                errorStr.append(errorWriter.toString());
                throw new DMCErrorDeleteFile(errorStr.toString());
            }
        }
        if (!filesWithDifferentSchema.isEmpty()) {
            schemaCounter++;
            chunksCounter = 1;
            List<MergedFile> mergedFiles = createMergedFileList(filesWithDifferentSchema);
            filesWithDifferentSchema.clear();
            mergeFiles(mergedFiles, outputDir);
        }
    }

    /*private ParquetWriter<GenericRecord> createParquetFile(String filePath, Schema schema, int rowGroupSize, CompressionCodecName compressionCodecName) throws IOException {
        return AvroParquetWriter.<GenericRecord>builder(new Path(filePath))
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(compressionCodecName)
                .withRowGroupSize(rowGroupSize)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
    }*/



}
