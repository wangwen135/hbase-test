package com.wwh.hbase.base;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDAO {
    private static Configuration conf = HBaseConfiguration.create();
    static {
        // conf.set("hbase.rootdir", "hdfs://hbase");
        // 设置Zookeeper,直接设置IP地址
        conf.set("hbase.zookeeper.quorum", "192.168.1.213,192.168.1.214,192.168.1.215");

        conf.set("hbase.zookeeper.property.clientPort", "2181");

        // conf.set("hbase.master", "192.168.1.100:600000");
    }

    // 创建表
    public static void createTable(String tablename, String columnFamily) throws Exception {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        TableName tableNameObj = TableName.valueOf(tablename);

        if (admin.tableExists(tableNameObj)) {
            System.out.println("Table exists!");
            System.exit(0);
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tablename));
            tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDesc);
            System.out.println("create table success!");
        }
        admin.close();
        connection.close();
    }

    // 删除表
    public static void deleteTable(String tableName) {
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            TableName table = TableName.valueOf(tableName);
            admin.disableTable(table);
            admin.deleteTable(table);
            System.out.println("delete table " + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 插入一行记录
    public static void addRecord(String tableName, String rowKey, String family, String qualifier, String value) {
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            table.close();
            connection.close();
            System.out.println("insert recored " + rowKey + " to table " + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void deleteRow(HTable table, String rowKey) throws Exception {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        System.out.println("Delete row: " + rowKey);
    }

    public static ResultScanner scanRange(HTable table, String startrow, String endrow) throws Exception {
        Scan s = new Scan(Bytes.toBytes(startrow), Bytes.toBytes(endrow));
        ResultScanner rs = table.getScanner(s);
        return rs;
    }

    public static void listTables() throws Exception {
        Connection connection = ConnectionFactory.createConnection(conf);

        HBaseAdmin hadmin = new HBaseAdmin(connection);

        TableName[] tnames = hadmin.listTableNames();

        for (TableName tableName : tnames) {
            System.out.println(tableName.getNameAsString());
        }

        System.out.println("============================================");

        HTableDescriptor[] HTableDesc = hadmin.listTables();
        for (HTableDescriptor hTableDescriptor : HTableDesc) {
            String name = hTableDescriptor.getNameAsString();
            System.out.println("表名称: " + name);
            System.out.println("类簇名称:");
            HColumnDescriptor[] columnDescriptors = hTableDescriptor.getColumnFamilies();
            for (HColumnDescriptor hColumnDescriptor : columnDescriptors) {
                System.out.println(hColumnDescriptor.getNameAsString());
            }
        }

        hadmin.close();

    }

    public void testGet() throws Exception {
        // HTablePool pool = new HTablePool(conf, 10);
        // HTable table = (HTable) pool.getTable("user");
        HTable table = new HTable(conf, "user");
        Get get = new Get(Bytes.toBytes("rk0001"));
        // get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        get.setMaxVersions(5);
        Result result = table.get(get);

        // String r = Bytes.toString(result.getValue(family, qualifier)
        // );这个获取方法不常用

        for (KeyValue kv : result.list()) {
            String family = new String(kv.getFamily());
            System.out.println(family);
            String qualifier = new String(kv.getQualifier());
            System.out.println(qualifier);
            System.out.println(new String(kv.getValue()));
        }
        table.close();
    }

    public static void testScan() throws Exception {
        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("users");
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        scan.setBatch(10);

        ResultScanner resultScanner = table.getScanner(scan);

        int i = 0;
        for (Result result : resultScanner) {
            i++;

            byte[] row = result.getRow();

            System.out.println("====================================");
            System.out.println("row id = " + Bytes.toString(row));

            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                byte[] family = cell.getFamily();
                System.out.println("family = " + Bytes.toString(family));

                byte[] qualifier = cell.getQualifier();
                System.out.println("qualifier = " + Bytes.toString(qualifier));

                byte[] values = cell.getValue();
                System.out.println("value = " + Bytes.toString(values));
            }

            if (i > 10) {
                break;
            }
        }
    }

    public void testDel() throws Exception {
        HTable table = new HTable(conf, "user");
        Delete del = new Delete(Bytes.toBytes("rk0001"));
        del.deleteColumn(Bytes.toBytes("data"), Bytes.toBytes("pic"));
        table.delete(del);
        table.close();
    }

    public static void testQuery() throws Exception {
        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("downloads");
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        // Cannot set batch on a scan using a filter that returns true for
        // filter.hasFilterRow
        // scan.setBatch(10);

        final byte[] family = Bytes.toBytes("cf1");
        scan.addFamily(family);

        scan.setFilter(new SingleColumnValueFilter(family, Bytes.toBytes("binaryContent"), CompareOp.EQUAL, Bytes.toBytes(true)));

        ResultScanner resultScanner = table.getScanner(scan);

        int i = 0;
        for (Result result : resultScanner) {
            i++;

            byte[] row = result.getRow();

            System.out.println("====================================");
            System.out.println("row id = " + Bytes.toString(row));

            byte[] bcontent = result.getValue(family, Bytes.toBytes("binaryContent"));
            System.out.println("binaryContent = " + Bytes.toBoolean(bcontent));

            byte[] contentType = result.getValue(family, Bytes.toBytes("contentType"));
            System.out.println("contentType = " + Bytes.toString(contentType));

            if (i > 10) {
                break;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // createTable("testTb", "info");
        // addRecord("testTb", "001", "info", "name", "zhangsan");
        // addRecord("testTb", "001", "info", "age", "20");
        // HbaseDao.deleteTable("testTb");

        // listTables();

        // testScan();

        testQuery();
    }
}
