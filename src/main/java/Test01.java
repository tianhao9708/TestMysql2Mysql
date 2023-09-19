import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Test01 {
    public static void main(String[] args) {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000).setParallelism(2);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sourceSql = "CREATE TABLE t1 (\n" +
                "  `stt` STRING,\n" +
                "  `edt` STRING,\n" +
                "  `id` STRING,\n" +
                "  `vc` STRING,\n" +
                "  PRIMARY KEY (`stt`,`id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "     'connector' = 'mysql-cdc',\n" +
                "     'hostname' = 'tianhao2',\n" +
                "     'port' = '3306',\n" +
                "     'username' = 'root',\n" +
                "     'password' = '000000',\n" +
                "     'database-name' = 'test',\n" +
                "     'table-name' = 'test'" +
                ")";
        String sinkSql = "CREATE TABLE t2 (" +
                "  `stt` STRING,\n" +
                "  `edt` STRING,\n" +
                "  `id` STRING,\n" +
                "  `vc` STRING,\n" +
                "  PRIMARY KEY (`stt`,`id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "'connector' = 'jdbc',\n" +
                "'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "'url' = 'jdbc:mysql://tianhao2:3306/test',\n" +
                "'username' = 'root',\n" +
                "'password' = '000000',\n" +
                "'table-name' = 'test2'\n" +
                ")";

        tEnv.executeSql(sourceSql);
        tEnv.executeSql(sinkSql);
        tEnv.executeSql("insert into t2 select * from t1");
    }
}
