package com.dcx.hbase; /**
 * Created by 党楚翔 on 2022/5/12.
 */

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

/**
 * @author: fz
 * @create: 2021-08-06 00:11
 **/
public class DropTable {
    public static void drop(Connection connection, String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        if (admin.isTableDisabled(TableName.valueOf(tableName))) {
            System.out.println("table不存在");
            return;
        }
        if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
        }
        admin.deleteTable(TableName.valueOf(tableName));
        System.out.println("成功删除表 " + tableName);
    }
}
