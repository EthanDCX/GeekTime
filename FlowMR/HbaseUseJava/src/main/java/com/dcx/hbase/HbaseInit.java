package com.dcx.hbase; /**
 * Created by 党楚翔 on 2022/5/11.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseInit {
    public static Connection createConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "emr-worker-2,emr-worker-1,emr-header-1");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
