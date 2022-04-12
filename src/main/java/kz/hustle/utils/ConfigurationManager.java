package kz.hustle.utils;

import kz.hustle.test.ConfigurationBuilder;
import org.apache.hadoop.conf.Configuration;

public class ConfigurationManager {
    public static Configuration getConf() {
        return ConfigurationBuilder.getHDFSConfiguration();
    };
}
