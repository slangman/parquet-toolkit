package kz.hustle.tools.sort;

import java.io.IOException;

public interface ParquetSorter {
    void sort(String field) throws Exception;
}
