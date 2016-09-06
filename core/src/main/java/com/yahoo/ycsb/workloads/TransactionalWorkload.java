package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.ConstantIntegerGenerator;
import com.yahoo.ycsb.generator.NumberGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.measurements.Measurements;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  NumberGenerator transactionlength;

  public static final String MAX_TRANSACTION_LENGTH_PROPERTY = "maxtransactionlength";
  public static final long MAX_TRANSACTION_LENGTH_DEFAULT = 10;

  public static final String TRANSACTION_LENGTH_DISTRIBUTION_PROPERTY = "transactionlengthdistribution";
  public static final String TRANSACTION_LENGTH_DISTRIBUTION_DEFAULT = "uniform";

  private static final Logger LOG = LoggerFactory.getLogger(TransactionalWorkload.class);

  @Override
  public void init(Properties p) throws WorkloadException {

    String transactionlengthdistrib;
    maxtransactionlength = Integer.parseInt(p.getProperty(MAX_TRANSACTION_LENGTH_PROPERTY,
      MAX_TRANSACTION_LENGTH_DEFAULT + ""));
    transactionlengthdistrib = p.getProperty(TRANSACTION_LENGTH_DISTRIBUTION_PROPERTY,
      TRANSACTION_LENGTH_DISTRIBUTION_DEFAULT);

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

    super.init(p);
  }


  @Override
  public boolean doTransaction(DB db, Object threadstate) {


    int transactionSize = transactionlength.nextValue().intValue(); //nextInt();
    LOG.info("dotransaction size {}", transactionSize);

    long st = System.nanoTime();
    Status res = db.startTransaction();

    if (res != Status.OK) // in case getting the timestamp fails - don't do the transaction
    {
      //TODO count conflicted transaction?
      return false;
    }

    for (int i = 0; i < transactionSize; ++i) {
      super.doTransaction(db, threadstate);
    }
    // Add a read cycle at the end - for checking performance effect only
    //super.doTransactionRead(db);

    db.commitTransaction();

    long en = System.nanoTime();

    Measurements.getMeasurements().measure("TRANSACTION", (int) ((en - st) / 1000));

    return true;
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    LOG.info("doInsert {}", Thread.currentThread().getId());

    if (db.startTransaction() != Status.OK) // in case getting the timestamp fails - don't do the transaction
      return false;

    if (super.doInsert(db,threadstate) == false)
      return false;

    if (db.commitTransaction() == Status.OK)
      return true;
    else
      return false;

  }

}
