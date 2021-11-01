package com.rsh.hbase.stockmarket.helper;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

/** Used by the book examples to generate tables and fill them with test data. */
public class HBaseHelper implements Closeable {

  private static HBaseHelper helper = null;
  private final Map<String, Table> tablesByName = new HashMap<>();
  private final Connection connection;

  private HBaseHelper(Configuration configuration) throws IOException {

    // Increase RPC timeout, in case of a slow computation
    configuration.setLong("hbase.rpc.timeout", 1000);
    // Default is 1, set to a higher value for faster scanner.next(..)
    configuration.setLong("hbase.client.scanner.caching", 1000);
    configuration.setLong("hbase.client.scanner.timeout.period", 120000);

    this.connection = ConnectionFactory.createConnection(configuration);
  }

  public static HBaseHelper getInstance(Configuration configuration) {
    if (helper == null) {
      try {
        helper = new HBaseHelper(configuration);
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }
    return helper;
  }

  public static HBaseHelper getInstance() {
    Configuration configuration = HBaseConfiguration.create();
    String path = HBaseHelper.class.getClassLoader().getResource("hbase-site.xml").getPath();
    configuration.addResource(new Path(path));
    return getInstance(configuration);
  }

  @Override
  public void close() throws IOException {
    connection.close();
  }

  public Table getTable(String tableName) {
    Table table = null;
    if (tablesByName.containsKey(tableName)) {
      table = tablesByName.get(tableName);
    } else {
      try {
        table = connection.getTable(TableName.valueOf(tableName));
        tablesByName.put(tableName, table);
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }
    return table;
  }
}
