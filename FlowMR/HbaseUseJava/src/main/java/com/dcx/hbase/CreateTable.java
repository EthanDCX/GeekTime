package com.dcx.hbase; /**
 * Created by 党楚翔 on 2022/5/11.
 */
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import java.io.IOException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class CreateTable {
    public static void create(Connection connection, String tableName, String... columnFamilies) throws IOException {
        Admin admin = connection.getAdmin();
        if (tableName == null || columnFamilies == null) {
            return;
        }
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        for (int i = 0; i < columnFamilies.length; i++) {
            if (columnFamilies[i] == null)
                continue;
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamilies[i]);
            columnDescriptor.setMaxVersions(1);
            table.addFamily(columnDescriptor);
        }
        admin.createTable(table);
        System.out.println("成功创建表 " + table + ", column family: " + Arrays.toString(columnFamilies));
    }
}


