package com.dcx.hbase;

/**
 * Created by 党楚翔 on 2022/5/12.
 */

import org.apache.hadoop.hbase.client.Connection;
import org.junit.Test;

import java.io.IOException;

public class CreateNameSpaceTest {
    @Test
    public void create() {
        try (Connection connection = HbaseInit.createConnection()) {
            CreateNameSpace.createNamespace(connection,"dcx");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}