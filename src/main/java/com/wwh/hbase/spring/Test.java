package com.wwh.hbase.spring;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

public class Test {

    public static void main(String[] args) throws IOException, Exception {
        AbstractApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");

        context.registerShutdownHook();

        HbaseTemplate hbaseTemplate = context.getBean(HbaseTemplate.class);

        Connection hbaseConnection = context.getBean(Connection.class);

        try (Admin admin = hbaseConnection.getAdmin()) {

            System.out.println("所有表名：");
            TableName[] tables = admin.listTableNames();
            for (TableName tableName : tables) {
                System.out.println(tableName.toString());
            }

        }

        Thread.sleep(5000);

        context.close();
    }
}
