package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.*;
import com.yahoo.ycsb.measurements.Measurements;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.or.ThreadGroupRenderer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;


/**
 * A transactional workload.
 * <p>
 * Properties to control the client:
 * </p>
 * <UL>
 * <LI><b>maxtransactionlength</b>: maximum number of operations per transaction (default 10)
 * <LI><b>transactionlengthdistribution</b>: what distribution should be used to choose the number of operations for
 * each transaction, between 1 and maxtransactionlength (default: uniform)
 * </ul>
 *
 */

public class TransactionalWorkload extends CoreWorkload {

  int maxtransactionlength;
  private Boolean isSingleton;
  NumberGenerator transactionlength;

  public static final String MAX_TRANSACTION_LENGTH_PROPERTY = "maxtransactionlength";

  public static final String RW_TX_PROPORTION_PROPERTY = "rwtxproportion";
  public static final String RW_TX_PROPORTION_PROPERTY_DEFAULT = "0.0";
  public static final String RMW_TX_PROPORTION_PROPERTY = "rmwtxproportion";
  public static final String RMW_TX_PROPORTION_PROPERTY_DEFAULT = "0.0";



  public static final long MAX_TRANSACTION_LENGTH_DEFAULT = 10;

  public static final String TRANSACTION_LENGTH_DISTRIBUTION_PROPERTY = "transactionlengthdistribution";
  public static final String TRANSACTION_LENGTH_DISTRIBUTION_DEFAULT = "uniform";

  public static final String SINGLETON_PROPERTY = "singletons";
  public static final String SINGLETON_DEFAULT = "false";

  private static final Logger LOG = LoggerFactory.getLogger(TransactionalWorkload.class);
  DiscreteGenerator transactionType;


  @Override
  public void init(Properties p) throws WorkloadException {

    String transactionlengthdistrib;
    maxtransactionlength = Integer.parseInt(p.getProperty(MAX_TRANSACTION_LENGTH_PROPERTY,
      MAX_TRANSACTION_LENGTH_DEFAULT + ""));
    transactionlengthdistrib = p.getProperty(TRANSACTION_LENGTH_DISTRIBUTION_PROPERTY,
      TRANSACTION_LENGTH_DISTRIBUTION_DEFAULT);

    isSingleton = Boolean.parseBoolean(p.getProperty(SINGLETON_PROPERTY, SINGLETON_DEFAULT));



    if (transactionlengthdistrib.equals("uniform")) {
      transactionlength = new UniformIntegerGenerator(1, maxtransactionlength);
    } else if (transactionlengthdistrib.equals("zipfian")) {
      transactionlength = new ZipfianGenerator(1, maxtransactionlength);
    } else if (transactionlengthdistrib.equals("constant")) {
      transactionlength = new ConstantIntegerGenerator(maxtransactionlength);
    } else {
      throw new WorkloadException("Distribution \"" + transactionlengthdistrib
        + "\" not allowed for transaction length");
    }
    transactionType = new DiscreteGenerator();

    double rwtx = Double.parseDouble(p.getProperty(RW_TX_PROPORTION_PROPERTY, RW_TX_PROPORTION_PROPERTY_DEFAULT));
    double rmwtx = Double.parseDouble(p.getProperty(RMW_TX_PROPORTION_PROPERTY, RMW_TX_PROPORTION_PROPERTY_DEFAULT));

    if (rwtx> 0) {
      transactionType.addValue(rwtx, "RW");
    }
    if (rmwtx> 0) {
      transactionType.addValue(rmwtx, "RMW");
    }


    super.init(p);
  }


  @Override
  public boolean doTransaction(DB db, Object threadstate) {

    int transactionSize = transactionlength.nextValue().intValue(); //nextInt();
    boolean singleTX = false;
    String operation = transactionType.nextString();
    if ((transactionSize == 1 || operation.equals("RMW")) && isSingleton) {
      singleTX = true;
    }

    long st = System.nanoTime();
    if (!singleTX) {
      Status res = db.startTransaction();
      if (res != Status.OK) // in case getting the timestamp fails - don't do the transaction
      {
        //TODO count conflicted transaction?
        return false;
      }
    }

    if (operation.equals("RMW")) {
      if (!singleTX) {
        super.doTransactionReadModifyWrite(db);
      } else {
        doTransactionReadModifyWriteLOCAL(db);
      }

    } else {
      for (int i = 0; i < transactionSize; ++i) {
        super.doTransaction(db, threadstate);
      }
    }


    // Add a read cycle at the end - for checking performance effect only
    //super.doTransactionRead(db);
    if (!singleTX)
    {
      long commitst = System.nanoTime();
      Status s = db.commitTransaction();
      long commiten = System.nanoTime();
      if (s == Status.OK) {
        if (operation.equals("RMW")) {
          Measurements.getMeasurements().measure(new String("COMMIT RMW"), (int) ((commiten - commitst) / 1000));
        } else {
          Measurements.getMeasurements().measure(new String("COMMIT SIZE " + Integer.toString(transactionSize)), (int) ((commiten - commitst) / 1000));
        }
      }
    }


    long en = System.nanoTime();

    if (operation.equals("RMW")) {
      if (singleTX) {
        Measurements.getMeasurements().measure(new String("TRANSACTION RMW LOCAL"), (int) ((en - st) / 1000));
      } else {
        Measurements.getMeasurements().measure(new String("TRANSACTION RMW"), (int) ((en - st) / 1000));
      }
    } else {
      Measurements.getMeasurements().measure(new String("TRANSACTION SIZE " + Integer.toString(transactionSize)), (int) ((en - st) / 1000));
    }
    Measurements.getMeasurements().measure("TRANSACTION", (int) ((en - st) / 1000));

    try {
      Thread.sleep(1);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return true;
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    LOG.info("doInsert {}", Thread.currentThread().getId());
    boolean res = true;
    if (db.startTransaction() != Status.OK) // in case getting the timestamp fails - don't do the transaction
      return false;

    for (int i = 0; i < Client._insertBatchSize && res == true; i++)
      res = super.doInsert(db,threadstate);

    if (db.commitTransaction() == Status.OK && res == true)
      return true;
    else
      return false;

  }


  public void doTransactionReadModifyWriteLOCAL(DB db) {
    // choose a random key
    int keynum = nextKeynum();

    String keyname = buildKeyName(keynum);

    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = super.fieldnames.get(fieldchooser.nextValue().intValue());

      fields = new HashSet<String>();
      fields.add(fieldname);
    }

    HashMap<String, ByteIterator> values;

    if (writeallfields) {
      // new data for all the fields
      values = super.buildValues(keyname);
    } else {
      // update a random field
      values = buildSingleValue(keyname);
    }

    // do the transaction

    HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();


    long ist = _measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    db.read(table, keyname, fields, cells);

    values.put("COUNTER_VALUE",cells.get("COUNTER_VALUE"));

    db.update(table, keyname, values);

    long en = System.nanoTime();

    if (dataintegrity) {
      verifyRow(keyname, cells);
    }

    _measurements.measure("READ-MODIFY-WRITE", (int) ((en - st) / 1000));
    _measurements.measureIntended("READ-MODIFY-WRITE", (int) ((en - ist) / 1000));
  }



}
