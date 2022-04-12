package kz.hustle.tools.common;

import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.util.List;

public interface InputSource {
    List<FileStatus> getFiles() throws IOException;
}
