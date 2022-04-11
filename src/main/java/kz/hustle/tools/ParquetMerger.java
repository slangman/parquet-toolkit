package kz.hustle.tools;

import kz.hustle.tools.merge.MergingNotCompletedException;

import java.io.IOException;

public interface ParquetMerger  {
    void merge() throws IOException, InterruptedException, MergingNotCompletedException;
}
