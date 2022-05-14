package com.dcx.hbase; /**
 * Created by 党楚翔 on 2022/5/11.
 */

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class PutRow {
    public static void insert(Connection connection, String tableName, String rowKey, String columnFamily, String column,
                              String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(columnFamily.getBytes(), column.getBytes(), value.getBytes());
        table.put(put);
    }
}