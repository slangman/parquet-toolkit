package kz.hustle.tools.common;

import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class InputPath implements InputSource{

    private String path;

    public InputPath(String path) {
        this.path = path;
    }


    @Override
    public List<FileStatus> getFiles() throws IOException {
        return (new InputFiles(Collections.singletonList(path))).getFiles();
    }
}
