package kz.hustle.utils;

import org.apache.hadoop.conf.Configuration;

public class DefaultConfigurationBuilder {
    public static Configuration getConf() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://nameservice1");
        configuration.set("fs.default.name", configuration.get("fs.defaultFS"));
        configuration.set("dfs.nameservices", "nameservice1");
        configuration.set("dfs.ha.namenodes.nameservice1", "namenode1,namenode2");
        //configuration.set("dfs.namenode.rpc-address.nameservice1.namenode1", "hdfs://kz-dmphdpname03:8020");
        //configuration.set("dfs.namenode.rpc-address.nameservice1.namenode2", "hdfs://kz-dmphdpname07:8020");
        configuration.set("dfs.namenode.rpc-address.nameservice1.namenode1", "localhost:8020");
        configuration.set("dfs.namenode.rpc-address.nameservice1.namenode2", "localhost:8021");
        configuration.set("dfs.client.failover.proxy.provider.nameservice1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("dfs.namenode.accesstime.precision", "0");
        configuration.set("hadoop.home.dir", "/");
        configuration.set("HADOOP_USER_NAME", "hdfs");
        return configuration;
    }
}
