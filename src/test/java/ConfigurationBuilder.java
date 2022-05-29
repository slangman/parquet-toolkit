import org.apache.hadoop.conf.Configuration;

//Только для тестирования, в сборку не включать
public class ConfigurationBuilder {
    public static Configuration getClouderaQuickstartConf() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://cloudera-vm:8020");
        configuration.set("fs.default.name", configuration.get("fs.defaultFS"));
        configuration.set("dfs.nameservices","nameservice1");
        configuration.set("dfs.socket.timeout", "1000");
        configuration.set("dfs.ha.namenodes.nameservice1", "namenode1");
        configuration.set("dfs.namenode.rpc-address.nameservice1.namenode1", "cloudera-vm:8020");
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("hadoop.home.dir", "/");
        return configuration;
    }
}
