package com.dcx.hbase;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

//创建表空间
public class CreateNameSpace {
    public static void createNamespace(Connection connection, String tablespace) throws IOException {
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        admin.createNamespace(NamespaceDescriptor.create(tablespace).build());
        System.out.println("成功创建表空间 " + tablespace);
    }
}