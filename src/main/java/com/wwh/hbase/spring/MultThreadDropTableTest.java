package com.wwh.hbase.spring;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

public class MultThreadDropTableTest {

    public static void main(String[] args) throws IOException, InterruptedException {

        AbstractApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");

        context.registerShutdownHook();

        HbaseTemplate hbaseTemplate = context.getBean(HbaseTemplate.class);

        Connection hbaseConnection = context.getBean(Connection.class);

        final String name = "testDel";

        try (Admin admin = hbaseConnection.getAdmin()) {

            TableName tableName = TableName.valueOf(name);

            if (!admin.tableExists(tableName)) {

                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

                HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toBytes("c"));
                tableDescriptor.addFamily(columnDescriptor);
                admin.createTable(tableDescriptor);

            }

            System.out.println("所有表名：");
            TableName[] tables = admin.listTableNames();
            for (TableName tName : tables) {
                System.out.println(tName.toString());
            }

        }

        Thread t1 = new Thread(new Runnable() {

            @Override
            public void run() {
                dropTable(hbaseConnection, name);
            }
        });
        Thread t2 = new Thread(new Runnable() {

            @Override
            public void run() {
                dropTable(hbaseConnection, name);
            }
        });

        // 测试两个线程同时删除表
        // org.apache.hadoop.hbase.TableNotEnabledException: testDel

        t1.start();

        t2.start();

        Thread.sleep(5000);

        context.close();

    }

    public static void dropTable(Connection hbaseConnection, String tName) {
        final TableName tableName = TableName.valueOf(tName);

        try (Admin admin = hbaseConnection.getAdmin()) {

            if (admin.tableExists(tableName)) {

                if (admin.isTableEnabled(tableName)) {

                    // 禁用表并等待其完成，可能超时
                    admin.disableTable(tableName);

                    // 删除表，同步操作
                    admin.deleteTable(tableName);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
