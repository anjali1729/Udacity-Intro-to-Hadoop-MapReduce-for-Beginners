package com.hbase.basic;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.conf.Configuration;

public class CreateTable {
      
   public static void main(String[] args) throws IOException {

      // Instantiating configuration class
      Configuration con = HBaseConfiguration.create();
      
      con.set("hbase.rootdir","/Applications/hbase-1.2.5");
      con.set("hbase.zookeeper.property.dataDir","/Applications/hbase-1.2.5/zookeeper");
      //con.set("hbase.zookeeper.quorum", "192.168.0.2");
      System.out.println("ANJALI : " + con.get("hbase.rootdir"));
      System.out.println("ANJALI : " + con.get("hbase.zookeeper.property.dataDir"));
      System.out.println("ANJALI : " + con.get("hbase.zookeeper.quorum"));
      System.out.println("ANJALI : " + con.get("hbase.zookeeper.property.clientPort"));

      // Instantiating HbaseAdmin class
      HBaseAdmin admin = new HBaseAdmin(con);

      // Instantiating table descriptor class
      HTableDescriptor tableDescriptor = new
      HTableDescriptor(TableName.valueOf("emp"));

      // Adding column families to table descriptor
      tableDescriptor.addFamily(new HColumnDescriptor("personal"));
      tableDescriptor.addFamily(new HColumnDescriptor("professional"));

      // Execute the table through admin
      admin.createTable(tableDescriptor);
      System.out.println(" Table created ");
   }
}