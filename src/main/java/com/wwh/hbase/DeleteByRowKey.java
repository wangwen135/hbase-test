package com.wwh.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

public class DeleteByRowKey {

    private static Configuration conf = HBaseConfiguration.create();

    static long count = 0;

    static {
        conf.set("hbase.zookeeper.quorum", "192.168.1.91,192.168.1.92,192.168.1.93");

        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    public static void queryAndDelete() throws Exception {

        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("downloads");
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        // Cannot set batch on a scan using a filter that returns true for
        // filter.hasFilterRow
        // scan.setBatch(10);

        final byte[] family = Bytes.toBytes("cf1");
        scan.addFamily(family);

        /**
         * <pre>
         * kpynyvym6xqi7wz2.onion  共删除数据：16871
         * dtt6tdtgroj63iud.onion 共删除数据：103082
         * 3cvpkfx4gdnkcduj.onion
         * bitmsgd3emeypwpj.onion/
         * </pre>
         */

        scan.setStartRow(Bytes.toBytes("bitmsgd3emeypwpj.onion"));
        scan.setStopRow(Bytes.toBytes("bitmsgd3emeypwpj.onion~"));

        ResultScanner resultScanner = table.getScanner(scan);

        for (Result result : resultScanner) {
            count++;

            byte[] row = result.getRow();

            System.out.println("====================================");
            System.out.println("del row id = " + Bytes.toString(row));

            // byte[] bcontent = result.getValue(family,
            // Bytes.toBytes("binaryContent"));
            // System.out.println("binaryContent = " +
            // Bytes.toBoolean(bcontent));
            //
            // byte[] contentType = result.getValue(family,
            // Bytes.toBytes("contentType"));
            // System.out.println("contentType = " +
            // Bytes.toString(contentType));

            Delete d = new Delete(row);
            table.delete(d);

        }

    }

    public static void main(String[] args) {
        try {
            queryAndDelete();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("共删除数据：" + count);
    }

}
