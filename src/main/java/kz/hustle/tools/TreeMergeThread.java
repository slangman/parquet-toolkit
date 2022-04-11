package kz.hustle.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;


import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

public class TreeMergeThread extends ParquetThread implements Runnable {
    private static final Logger logger = LogManager.getLogger(TreeMultithreadedParquetMerger.class);
    private List<MergedFile> files;
    private String outputDir;
    private String outputFileName;
    private Path outputFilePath;
    private MultithreadedParquetMerger merger;
    private Configuration configuration;
    private FileSystem fs;
    private boolean removeInputFiles;

    public TreeMergeThread(TreeMultithreadedParquetMerger merger,
                           List<MergedFile> files,
                           String outputDir,
                           boolean removeInputFiles) {
        this.files = files;
        this.outputDir = outputDir;
        this.merger = merger;
        configuration = merger.getConf();
        fs = merger.getFs();
        this.removeInputFiles = removeInputFiles;
    }

    @Override
    public void run() {
        String errorMessage;
        try {
            mergeFiles(files, new Path(outputDir));
        } catch (IOException e1) {
            errorMessage = e1.getMessage();
            logger.error(errorMessage);
            e1.printStackTrace();
            try {
                if (fs.exists(outputFilePath)) {
                    logger.info("Removing temporary file: " + outputFileName);
                    fs.delete(outputFilePath, false);
                }
            } catch (IOException e) {
                logger.error(e1.getLocalizedMessage());
            }
        } catch (DMCErrorRenameFile e2) {
            logger.error("MERGE ERROR: " + outputDir);
            logger.error(e2.getMessage());
            e2.printStackTrace();
            try {
                if (fs.exists(outputFilePath)) {
                    System.out.println("DMCErrorRenameFile: Удаление временного файла");
                    fs.delete(outputFilePath, false);
                }
            } catch (IOException e) {
                System.out.println(e.getLocalizedMessage());
            }
        } catch (DMCErrorAppendFileToParquet | DMCErrorDeleteFile e3) {
            logger.error("MERGE ERROR: " + outputDir);
            logger.error(e3.getMessage());
            e3.printStackTrace();
        }
    }

    private void mergeFiles(List<MergedFile> inputFiles, Path outputDir)
            throws IOException,
            DMCErrorAppendFileToParquet,
            DMCErrorDeleteFile,
            DMCErrorRenameFile {
        String fileName;
        StringBuilder errorStr = new StringBuilder();
        String dirName = outputDir.toString();
        fileName = inputFiles.get(0).getFileName() + "_merger_";
        outputFileName = dirName + "/" + fileName;
        outputFilePath = new Path(outputFileName);
        String finalFileName = outputFileName.replace("_merger_", "");
        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, false);
        }
        ParquetMetadata readFooter;
        try {
            readFooter = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(inputFiles.get(0).getPath()), configuration)).getFooter();
            //readFooter = ParquetFileReader.readFooter(configuration, new Path(inputFiles.get(0).getPath()), ParquetMetadataConverter.NO_FILTER);
        } catch (RuntimeException e) {
            e.printStackTrace();
            merger.brokenFiles.add(inputFiles.get(0).getPath());
            System.out.println(inputFiles.get(0).getPath() + "  is not a Parquet file, will be skipped.");
            return;
        }
        FileMetaData fileMetadata = readFooter.getFileMetaData();
        //TODO: constructor is deprecated, replace with actual
        ParquetFileWriter writer = new ParquetFileWriter(configuration, readFooter.getFileMetaData().getSchema(), outputFilePath, ParquetFileWriter.Mode.CREATE);
        writer.start();
        int partsCounter = 0;
        for (MergedFile inputFile : inputFiles) {
            try {
                logger.debug("Appending part " + partsCounter);
                writer.appendFile(HadoopInputFile.fromPath(new Path(inputFile.getPath()), configuration));
                inputFile.setMustBeDeleted(true);
            } catch (Exception e) {
                StringWriter errorWriter = new StringWriter();
                e.printStackTrace(new PrintWriter(errorWriter));
                System.out.println("APPENDING FILE ERROR: " + e.getLocalizedMessage());
                errorStr.append("ADDING FILE ERROR: " + inputFile.toString() + "\n");
                errorStr.append(errorWriter.toString());
                if (fs.exists(new Path(finalFileName))) {
                    fs.delete(new Path(finalFileName), false);
                }
                throw new DMCErrorAppendFileToParquet(errorStr.toString());
            }
            partsCounter++;
        }
        writer.end(fileMetadata.getKeyValueMetaData());
        if (removeInputFiles) {
            logger.debug("Removing files to merge...");
            for (MergedFile input : inputFiles) {
                try {
                    if (input.mustBeDeleted()) {
                        fs.delete(new Path(input.getPath()), false);
                    }
                } catch (Exception e) {
                    System.out.println("REMOVING FILE ERROR: " + e.getLocalizedMessage());
                    StringWriter errorWriter = new StringWriter();
                    e.printStackTrace(new PrintWriter(errorWriter));
                    errorStr.append("DELETING FILE ERROR: " + input.toString() + "\n");
                    errorStr.append(errorWriter.toString());
                    throw new DMCErrorDeleteFile(errorStr.toString());
                }
            }
        }
        try {
            fs.rename(outputFilePath, new Path(finalFileName));
        } catch (Exception e) {
            StringWriter errorWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(errorWriter));
            errorStr.append("RENAMING FILE ERROR: " + outputFileName + "\n");
            errorStr.append("RENAMING FILE ERROR: " + finalFileName + "\n");
            errorStr.append(errorWriter.toString());
            throw new DMCErrorRenameFile(errorStr.toString());
        }
    }

    /*private void appendFiles(ParquetFileWriter writer, List<MergedFile> inputFiles) {
        int partsCounter = 0;
        for (MergedFile inputFile : inputFiles) {
            try {
                logger.debug("Appending part " + partsCounter);
                //TODO: method is deprecated, replace with actual
                writer.appendFile(configuration, new Path(inputFile.getPath()));
                inputFile.setMustBeDeleted(true);
            } catch (Exception e) {
                StringWriter errorWriter = new StringWriter();
                e.printStackTrace(new PrintWriter(errorWriter));
                System.out.println("APPENDING FILE ERROR: " + e.getLocalizedMessage());
                errorStr.append("ADDING FILE ERROR: " + inputFile.toString() + "\n");
                errorStr.append(errorWriter.toString());
                if (fs.exists(new Path(finalFileName))) {
                    fs.delete(new Path(finalFileName), false);
                }
                throw new DMCErrorAppendFileToParquet(errorStr.toString());
            }
            partsCounter++;
        }
    }*/
}
