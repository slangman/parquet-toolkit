package kz.hustle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class DMCParquet {

    private Configuration configuration;

    public DMCParquet(Configuration configuration) {
        this.configuration = configuration;
    }

    /*public ParquetFile open(Path path){
        return new ParquetFile(path);
    }*/

    public ParquetFolder openFolder(Path path) {
        return new ParquetFolder(path, configuration);
    }
}
