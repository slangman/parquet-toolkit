package kz.hustle.tools.sort;

//import kz.dmc.packages.threads.pools.DMCThreadsPool;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;

public interface FileProcessor {
    void execute(FileStatus file) throws IOException;

    default void setFactoryPool(SortThreadPool pool) {
        throw new UnsupportedOperationException();
    }

    default void setPartFilePool(SortThreadPool pool) {
        throw new UnsupportedOperationException();
    }

    default void setPartFileSize(long size) {
        throw new UnsupportedOperationException();
    }
}
