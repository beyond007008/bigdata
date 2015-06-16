package com.neusoft.util;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtil {

	// initial
	static Configuration config = null;
	static {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum",
				"10.10.11.97, 10.10.11.98, 10.10.11.100");
		config.set("hbase.zookeeper.property.clientPort", "2181");
	}

	// create a new table
	public void createTable(String tableName, String[] familys)
			throws IOException {
		System.out.println("111111111111----------");
		HBaseAdmin admin = new HBaseAdmin(config);

		System.out.println("----------");
		if (admin.tableExists(tableName)) {
			System.out.println(tableName
					+ " is already exists,Please create another table!");
		} else {
			HTableDescriptor desc = new HTableDescriptor(tableName);
			for (int i = 0; i < familys.length; i++) {
				HColumnDescriptor family = new HColumnDescriptor(familys[i]);
				desc.addFamily(family);
			}
			admin.createTable(desc);
			System.out.println("Create table \'" + tableName + "\' OK!");
		}

	}

	// delete a table
	public void deleteTable(String tableName) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(config);
		if (!admin.tableExists(tableName)) {
			System.out.println(tableName + " is not exists!");
		} else {
			Scanner s = new Scanner(System.in);
			System.out.print("Are you sure to delete(y/n)?");
			String str = s.nextLine();
			if (str.equals("y") || str.equals("Y")) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				System.out.println(tableName + " is delete!");
			} else {
				System.out.println(tableName + " is not delete!");
			}
		}
	}

	// Get table example
	public void getTable(String tableName) throws IOException {
		// Method I:
		// HTable table = new HTable(config, tableName);
		// Method II: better than I.
		HTablePool pool = new HTablePool(config, 1000);
		@SuppressWarnings("unused")
		HTable table = (HTable) pool.getTable(tableName);
	}

	// add a data
	public void insertData(String tableName, String rowKey, String family,
			String qualifier, String value) {
		try {
			HTablePool pool = new HTablePool(config, 1000);
			HTable table = (HTable) pool.getTable(tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
					Bytes.toBytes(value));
			table.put(put);
			System.out.println("insert a data successful!");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// delete a data
	public void deleteData(String tableName, String rowKey) {
		try {
			HTablePool pool = new HTablePool(config, 1000);
			HTable table = (HTable) pool.getTable(tableName);
			Delete del = new Delete(Bytes.toBytes(rowKey));
			table.delete(del);
			System.out.println("delete a data successful");
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// Query all data
	public void queryAll(String tableName) {
		try {
			HTablePool pool = new HTablePool(config, 1000);
			HTable table = (HTable) pool.getTable(tableName);
			Scan scan = new Scan();
			ResultScanner scanner = table.getScanner(scan);

			for (Result row : scanner) {
				System.out.println("\nRowkey: " + new String(row.getRow()));
				for (KeyValue kv : row.raw()) {
					System.out.print(new String(kv.getRow()) + " "); // same as
																		// above
					System.out.print(new String(kv.getFamily()) + ":");
					System.out.print(new String(kv.getQualifier()) + " = ");
					System.out.print(new String(kv.getValue()));
					System.out
							.print(" timestamp = " + kv.getTimestamp() + "\n");
				}
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// query by rowkey
	public void queryByRowKey(String tableName, String rowKey) {
		try {
			HTablePool pool = new HTablePool(config, 1000);
			HTable table = (HTable) pool.getTable(tableName);
			Get get = new Get(rowKey.getBytes());
			Result row = table.get(get);
			for (KeyValue kv : row.raw()) {
				System.out.print(new String(kv.getRow()) + " ");
				System.out.print(new String(kv.getFamily()) + ":");
				System.out.print(new String(kv.getQualifier()) + " = ");
				System.out.print(new String(kv.getValue()));
				System.out.print(" timestamp = " + kv.getTimestamp() + "\n");
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		

//		System.loadLibrary("lib");
		System.out.println(System.getProperty("java.library.path"));  
		HBaseUtil hbase = new HBaseUtil();
		// Create a new table
		String tableName = "test";
		System.out.println("=======delete table======");
		// hbase.deleteTable(tableName);

		System.out.println("=======create table======");
		String[] familys = { "info", "scores" };
		hbase.createTable(tableName, familys);

		System.out.println("=======insert data=======");
		// insert Jim
		// hbase.insertData(tableName, "Jim", "info", "sex", "male");
		// hbase.insertData(tableName, "Jim", "info", "age", "18");
		// hbase.insertData(tableName, "Jim", "scores", "Chinese", "98");
		// hbase.insertData(tableName, "Jim", "scores", "English", "90");
		// hbase.insertData(tableName, "Jim", "scores", "Math", "100");
		// //insert Ann
		// hbase.insertData(tableName, "Ann", "info", "sex", "female");
		// hbase.insertData(tableName, "Ann", "info", "age", "18");
		// hbase.insertData(tableName, "Ann", "scores", "Chinese", "97");
		// hbase.insertData(tableName, "Ann", "scores", "Math", "95");
		//
		// System.out.println("=======query all data=======");
		// hbase.queryAll(tableName);
		//
		// System.out.println("=======query by rowkey=======");
		// String rowKey = "Ann";
		// hbase.queryByRowKey(tableName, rowKey);
		//
		// System.out.println("=======delete a data=======");
		// hbase.deleteData(tableName, rowKey);
		// hbase.queryAll(tableName);

	}

}
