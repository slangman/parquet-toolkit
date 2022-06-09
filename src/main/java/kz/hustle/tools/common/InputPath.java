package kz.hustle.tools.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class InputPath implements InputSource{

    private String path;

    public InputPath(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    @Override
    public List<FileStatus> getFiles(Configuration conf) throws IOException {
        return (new InputFiles(Collections.singletonList(path))).getFiles(conf);
    }
}
