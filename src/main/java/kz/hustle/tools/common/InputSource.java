package kz.hustle.tools.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.util.List;

public interface InputSource {
    List<FileStatus> getFiles(Configuration conf) throws IOException;
}
