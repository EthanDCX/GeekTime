package com.dcx.hbase;

import org.apache.hadoop.hbase.client.Connection;
import org.junit.Test;
import java.io.IOException;

/**
 * Created by 党楚翔 on 2022/5/12.
 */
public class CreateTableTest {


    @Test
    public void create() {
        try (Connection connection = HbaseInit.createConnection()) {
            CreateTable.create(connection,"dcx:student", "name", "info", "score");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
