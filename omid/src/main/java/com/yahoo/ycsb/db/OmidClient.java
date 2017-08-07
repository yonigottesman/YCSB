package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.measurements.Measurements;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.omid.transaction.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * YCSB binding for omid.
 *
 * See {@code omid/README.md} for details.
 */
public class OmidClient extends DB {

  private static final Logger LOG = LoggerFactory.getLogger(OmidClient.class);
  private TransactionManager transactionManager;
  private Transaction transactionState = null;

  private TTable lastHTable = null;
  private byte[] columnFamily = null;

  private final Configuration hBaseConfig = HBaseConfiguration.create();

  private static HConnection hConn = null;
  private static final AtomicInteger THREAD_COUNT = new AtomicInteger(0);

  public static final String BUFFER_WRITE_PROPERTY = "bufferwrites";
  public static final String BUFFER_WRITE_DEFAULT = "false";
  private Boolean bufferWrites = false;

  private final Object tableLock = new Object();
  private int txSize;

  public void init() throws DBException {

    if ((getProperties().getProperty("debug") != null) &&
        (getProperties().getProperty("debug").compareTo("true") == 0)) {
      org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
    } else {
      org.apache.log4j.Logger.getRootLogger().setLevel(Level.OFF);
    }

    bufferWrites= Boolean.parseBoolean(getProperties().getProperty(BUFFER_WRITE_PROPERTY, BUFFER_WRITE_DEFAULT));

    //TODO change to support multiple columnfamily
    columnFamily = Bytes.toBytes(getProperties().getProperty("columnfamily"));
    if (columnFamily == null) {
      System.err.println("Error, must specify a columnfamily for HBase table");
      throw new DBException("No columnfamily specified");
    }

    try {
      HBaseOmidClientConfiguration omidClientConfiguration = new HBaseOmidClientConfiguration();
      //omidClientConfiguration.setPostCommitMode(OmidClientConfiguration.PostCommitMode.ASYNC);
      transactionManager = HBaseTransactionManager.newInstance(omidClientConfiguration);
    } catch (Exception e) {
      throw new DBException(e);
    }

    try {
      THREAD_COUNT.getAndIncrement();
      synchronized(THREAD_COUNT) {
        if (hConn == null){
          hConn = HConnectionManager.createConnection(hBaseConfig);
        }
      }
    } catch (IOException e) {
      System.err.println("Connection to HBase was not successful");
      throw new DBException(e);
    }

    bufferWrites= Boolean.parseBoolean(getProperties().getProperty(BUFFER_WRITE_PROPERTY, BUFFER_WRITE_DEFAULT));

    Properties props = getProperties();
    LOG.info("OmidClient:init\n");
  }


  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table
   *            The name of the table
   * @param key
   *            The record key of the record to read.
   * @param fields
   *            The list of fields to read, or null for all of them
   * @param result
   *            A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    txSize++;
    //LOG.info("Doing read from HBase {}:{}", Bytes.toString(columnFamily), key);
    if (lastHTable == null || !(Bytes.toString(lastHTable.getTableName()).equals(table))) {
      try {
        getHTable(table);
      } catch (IOException e) {
        System.err.println("Error accessing HBase table: " + e);
        return Status.SERVICE_UNAVAILABLE;
      }
    }

    Result res = null;


    Get get = new Get(Bytes.toBytes(key));
    if (fields == null) {
      get.addFamily(columnFamily);
    } else {
      for (String field : fields) {
        get.addColumn(columnFamily, Bytes.toBytes(field));
      }
    }

    try {
      if (transactionState == null) {
        System.err.println("Error reading outside of transaction, is this ok?");
      } else {
        res = lastHTable.get(transactionState, get);
      }
    } catch (IOException e) {
      System.err.println("Error doing get: " + e);
      return Status.SERVICE_UNAVAILABLE;
    }

    if (!res.isEmpty()) {
      for (Cell cell : res.listCells()) {
        result.put(cell.getQualifierArray().toString(), new ByteArrayByteIterator(cell.getValueArray()));
        //LOG.info("Result for field {} ", Bytes.toString(cell.getQualifierArray()));
      }
    }

    return Status.OK;
  }

  public Status scan(String s, String s1, int i, Set<String> set, Vector<HashMap<String, ByteIterator>> vector) {
    //LOG.info("scan");
    return Status.NOT_IMPLEMENTED;

  }

  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    txSize++;
    //LOG.info("Doing update to DB");

    if (lastHTable == null || !(Bytes.toString(lastHTable.getTableName()).equals(table))) {
      try {
        getHTable(table);
      } catch (IOException e) {
        System.err.println("Error accessing HBase table: " + e);
        return Status.SERVICE_UNAVAILABLE;
      }
    }

    Put put = new Put(Bytes.toBytes(key));
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      put.add(columnFamily, Bytes.toBytes(entry.getKey()), entry.getValue().toArray());
    }

    try {
      if (transactionState == null) {
        System.err.println("Error writing outside of transaction, is this ok?");
        return Status.SERVICE_UNAVAILABLE;
      } else {
        lastHTable.put(transactionState, put);
      }
    } catch (IOException e) {

      System.err.println("Error doing get: " + e);
      return Status.SERVICE_UNAVAILABLE;
    }

    return Status.OK;
  }

  public Status insert(String s, String s1, HashMap<String, ByteIterator> hashMap) {
    return update(s, s1, hashMap);
  }

  public Status delete(String table, String key) {
    //LOG.info("delete");

    if (lastHTable == null || !(Bytes.toString(lastHTable.getTableName()).equals(table))) {
      try {
        getHTable(table);
      } catch (IOException e) {
        System.err.println("Error accessing HBase table: " + e);
        return Status.SERVICE_UNAVAILABLE;
      }
    }

    Delete delete = new Delete(Bytes.toBytes(key));
    try {
      if (transactionState == null) {
        System.err.println("Error deleting outside of transaction");
        return Status.SERVICE_UNAVAILABLE;
      } else {
        lastHTable.delete(transactionState, delete);
      }
    } catch (IOException e) {
      System.err.println("Error doing delete: " + e);
      return Status.SERVICE_UNAVAILABLE;
    }

    return Status.OK;
  }


  /**
   * Starts a new transaction. All operations performed until abortTransaction() or commitTransaction() are called
   * belong to the same transaction.
   */
  @Override
  public Status startTransaction() {
    txSize=0;
    //LOG.info("startTransactions");

    try {
      transactionState = transactionManager.begin();
      //LOG.info("Transaction {} started", transactionState);
    } catch (TransactionException e) {
      System.err.println("begin transaction failed" + e.getMessage() + e.getCause().getMessage());
      return Status.SERVICE_UNAVAILABLE;
    }

    return Status.OK;
  }

  /**
   * Commits the current transaction.
   */
  @Override
  public Status commitTransaction() {

    //LOG.info("commitTransaction");

    try {

      long commitst = System.nanoTime();
      transactionManager.commit(transactionState);
      long commiten = System.nanoTime();

      if (transactionState.getStatus() == Transaction.Status.COMMITTED_RO) {
        Measurements.getMeasurements().measure(new String("COMMIT_RO"), (int) ((commiten - commitst) / 1000));
        Measurements.getMeasurements().measure(new String("COMMIT SIZE_RO " + Integer.toString(txSize)),
            (int) ((commiten - commitst) / 1000));
      } else {
        Measurements.getMeasurements().measure(new String("COMMIT SIZE_RW " + Integer.toString(txSize)),
            (int) ((commiten - commitst) / 1000));
        Measurements.getMeasurements().measure(new String("COMMIT_RW"), (int) ((commiten - commitst) / 1000));
      }

      //LOG.info("Transaction {} ended TRUE", transactionState.getTransactionId());
    } catch (TransactionException e) {
      System.err.println("commit transaction failed" + e.getMessage());
      return Status.SERVICE_UNAVAILABLE;
    } catch (RollbackException e) {
      LOG.info("Transaction {} ended FALSE {}", transactionState, e.getMessage());
      return Status.BAD_REQUEST;
    } finally {
      transactionState = null;
    }

    return Status.OK;
  }

  /**
   * Aborts the current transaction.
   */
  @Override
  public Status abortTransaction() {
    try {
      transactionManager.rollback(transactionState);
    } catch (TransactionException e) {
      return Status.SERVICE_UNAVAILABLE;
    } finally {
      transactionState = null;
    }
    LOG.info("abortTransactions");
    return Status.OK;
  }
  
  @Override
  public void cleanup() {
    //Dont clode TM ot talbe so that ASYNC thread can continue working
    //transactionManager.close();
    //lastHTable.close();
  }

  private void getHTable(String table) throws IOException {
    //TODO why is this syncronized?! what happends if multiple threads change _htable and use
    synchronized (tableLock) {
      //lastHTable = new TTable(hBaseConfig, table);
      lastHTable = new TTable(hConn.getTable(table));
      if (bufferWrites) {
        lastHTable.setWriteBufferSize(1024 * 1024 * 12);
        lastHTable.setAutoFlush(false);
      }

    }
  }



}
