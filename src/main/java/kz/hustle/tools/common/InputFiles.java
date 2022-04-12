package kz.hustle.tools.common;

import kz.hustle.utils.ConfigurationManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.hadoop.util.HiddenFileFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InputFiles implements InputSource {

    private List<String> input;
    private Configuration conf;

    public InputFiles(List<String> input) {
        this.conf = ConfigurationManager.getConf();
    }

    @Override
    public List<FileStatus> getFiles() throws IOException {
        List<FileStatus> inputFiles = null;
        if (input.size() == 1) {
            Path p = new Path(input.get(0));
            FileSystem fs = p.getFileSystem(conf);
            FileStatus status = fs.getFileStatus(p);

            if (status.isDir()) {
                inputFiles = getInputFilesFromDirectory(status);
            }
        } else {
            inputFiles = parseInputFiles(input);
        }
        checkParquetFiles(inputFiles);

        return inputFiles;
    }

    private void checkParquetFiles(List<FileStatus> inputFiles) throws IOException {
        if (inputFiles == null || inputFiles.size() <= 1) {
            throw new IllegalArgumentException("Not enough files to merge");
        }

        for (FileStatus status: inputFiles) {
            if (status.isDir()) {
                throw new IllegalArgumentException("Illegal parquet file: " + status.getPath().toString());
            }
        }
    }

    private List<FileStatus> getInputFilesFromDirectory(FileStatus partitionDir) throws IOException {
        FileSystem fs = partitionDir.getPath().getFileSystem(conf);
        FileStatus[] inputFiles = fs.listStatus(partitionDir.getPath(), HiddenFileFilter.INSTANCE);
        List<FileStatus> input = new ArrayList<>();
        Collections.addAll(input, inputFiles);
        return input;
    }

    private List<FileStatus> parseInputFiles(List<String> input) throws IOException {
        List<FileStatus> inputFiles = new ArrayList<>();

        for (String name : input) {
            Path p = new Path(name);
            FileSystem fs = p.getFileSystem(conf);
            inputFiles.add(fs.getFileStatus(p));
        }
        return inputFiles;
    }
}
