package kz.hustle.tools.merge;

import kz.hustle.tools.common.ParquetThread;
import kz.hustle.tools.exception.DMCErrorDeleteFile;
import kz.hustle.tools.exception.DMCErrorRenameFile;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

/*TODO: 1. Разобраться с эксепшенами. Нужно чтобы при возникновении критического эксепшена в потоке все остальные потоки
        тоже останавливались.
        2. При мёрджинге из папки удаляются файлы, не участвующую в мёрджинге (напр. файл _SUCCESS), такого не должно быть
        3. В логи выводится двойной слэш:
        File /bigdata/datamart/address_fttb/monthly/dict_eprysis/_time_key=2020-05-01/merged//merged-datafile-part-1.parq merged successfully.
 */
public class SimpleMergeThread extends ParquetThread implements Runnable {
    private final List<MergedFile> files;
    private final String outputDir;
    private final String outputFileName;
    private final MultithreadedParquetMerger merger;
    private final Configuration conf;
    private final FileSystem fs;
    private final int chunkNumber;
    private final int schemaNumber;
    private final int outputRowGroupSize;
    private final CompressionCodecName compressionCodecName;
    private final Schema schema;
    private final int badBlockReadAttempts;
    private final long badBlockReadTimeout;
    private final int allDatanodesAreBadReadAttempts;
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleMergeThread.class);

    public SimpleMergeThread(SimpleMultithreadedParquetMerger merger,
                             List<MergedFile> files,
                             Schema schema,
                             String outputDir,
                             String outputFileName,
                             int chunkNumber,
                             int schemaNumber,
                             int outputRowGroupSize,
                             CompressionCodecName compressionCodecName,
                             int badBlockReadAttempts,
                             long badBlockReadTimeout) throws IOException {
        this.files = files;
        this.schema = schema;
        this.outputDir = outputDir;
        this.outputFileName = outputFileName;
        this.merger = merger;
        conf = merger.getConf();
        this.fs = DistributedFileSystem.get(conf);
        this.chunkNumber = chunkNumber;
        this.schemaNumber = schemaNumber;
        this.outputRowGroupSize = outputRowGroupSize;
        this.compressionCodecName = compressionCodecName;
        this.badBlockReadAttempts = badBlockReadAttempts;
        this.badBlockReadTimeout = badBlockReadTimeout;
        this.allDatanodesAreBadReadAttempts = 3;
    }


    @Override
    public void run() {
        int attempts = 0;
        boolean success = false;
        while (!success) {
            success = true;
            try {
                mergeFiles(attempts != 0);
            } catch (IOException | InterruptedException | DMCErrorRenameFile | DMCErrorDeleteFile e) {
                if (allDatanodesAreBad(e) && attempts <= allDatanodesAreBadReadAttempts) {
                    e.printStackTrace();
                    LOGGER.info("Retry merging files. Attempt " + (++attempts));
                    success = false;
                } else {
                    LOGGER.error("MERGE ERROR: " + outputDir);
                    LOGGER.error(e.getMessage());
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        }

    }

    private void mergeFiles(boolean retry)
            throws IOException,
            InterruptedException,
            DMCErrorDeleteFile,
            DMCErrorRenameFile {
        if (files == null || files.isEmpty()) {
            return;
        }
        StringBuilder errorStr = new StringBuilder();
        /*Schema schema = null;
        String firstFilePath = files.get(0).getPath();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(HadoopInputFile.fromPath(new Path(firstFilePath), conf))
                .withConf(conf)
                .build();) {
            GenericRecord record = reader.read();
            schema = record.getSchema();
        } catch (IOException e) {
            //brokenFiles.add(firstFilePath);
        }*/
        if (schema != null) {
            String mergedFileName = (outputDir + "/"
                    + outputFileName
                    + (schemaNumber > 0 ? ("-schema-" + schemaNumber) : "")
                    + "-part-" + chunkNumber
                    + ".parq_merger_");
            if (retry) {
                if (fs.exists(new Path(mergedFileName))) {
                    fs.delete(new Path(mergedFileName), false);
                }
            }
            //modifySchema();
            ParquetWriter<GenericRecord> writer = merger.createParquetFile(mergedFileName, schema, outputRowGroupSize, compressionCodecName);
            AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<>();
            for (MergedFile file : files) {
                if (file.getLength() == 0) {
                    continue;
                }
                ParquetReader.Builder<GenericRecord> builder = ParquetReader.builder(readSupport, new Path(file.getPath()));
                ParquetReader<GenericRecord> reader = builder.withConf(conf).build();
                /*ParquetReader<GenericRecord> reader = AvroParquetReader
                        .<GenericRecord>builder(HadoopInputFile.fromPath(new Path(file.getPath()), conf))
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
                            /*reader = AvroParquetReader
                                    .<GenericRecord>builder(HadoopInputFile.fromPath(new Path(file.getPath()), conf))
                                    .withConf(conf)
                                    .build();*/
                            reader = builder.withConf(conf).build();
                            e.printStackTrace();
                            LOGGER.info("Retry reading file. Attempt " + (++attempts));
                            success = false;
                        } else {
                            throw e;
                        }
                    }
                }
                if (record == null) {
                    continue;
                }
                Schema schema1 = record.getSchema();
                if (schema1.equals(schema)) {
                    while (record != null) {
                        writer.write(record);
                        record = reader.read();
                    }
                    file.setMustBeDeleted(true);
                } else {
                    merger.filesWithDifferentSchema.add(file.getPath());
                }
                reader.close();
            }
            writer.close();
            try {
                merger.alreadyMerged.add(mergedFileName.replace("_merger_", ""));
                LOGGER.info("File " + mergedFileName.replace("_merger_", "") + " merged successfully.");
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
        /*for (MergedFile file : files) {
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
        }*/
    }

    private boolean allDatanodesAreBad(Exception e) {
        String message = e.getMessage();
        return (message.matches("All datanodes \\[.+\\] are bad\\. Aborting\\.\\.\\.")
                || message.matches("[cC]an not write PageHeader\\(.*\\)"));
    }

    private void modifySchema(Schema unmodifiedSchema) {
        unmodifiedSchema.getFields().forEach(field-> {
            System.out.println(field);
        });
    };
}
