import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.fs.FSDataInputStream;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public abstract class CAWriter implements Runnable {

  public abstract void run();
  public Cluster cluster;
  public Session session;
  
  String method = "CAWriter";
  String source_file;
  String dest_file;
  int threadNum;
  Thread threads[];

  CountDownLatch countDownLatch;
  ReentrantReadWriteLock readWriteLock;
  Lock destFileLock;
  Condition writeCondition;

  FSDataInputStream fsi = null;
  BufferedOutputStream bo = null;
  int bufSize = 1024 * 1024;
  int written = 0;
  long len;
  long index;
  public CAWriter(String method) {
    this.method = method;
    destFileLock = new ReentrantLock();
    writeCondition = destFileLock.newCondition();
    index = 0;
    try {
      cluster = Cluster.builder().addContactPoints("148.100.92.159").withPort(4392).withCredentials("user02", "1452983")
          .build();
      session = cluster.connect("keyspace_user02");
    } catch (Exception e) {
      System.out.println("connect fail");
      e.printStackTrace();
    }
  }

  public Session getSession() {
    return session;
  }
  public void createCF(String cf) {
    String query = "DROP TABLE  IF EXISTS keyspace_user02." + dest_file + " ;";
    ResultSet rs =  session.execute(query);
    System.out.println(rs);
    String createTableCQL = "create table if not exists keyspace_user02." + dest_file+"(i int PRIMARY KEY, data set<int>)";
    rs = session.execute(createTableCQL);
    System.out.println(rs);
  }
  public void set(String source_file, String dest_file, int num) {
    this.source_file = source_file;
    this.dest_file = dest_file;
    this.threadNum = num;
    this.countDownLatch = new CountDownLatch(num);
    this.createCF(dest_file);
    this.written = 0;
    try {
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  public void size() {
    String cql = "select count(*) as coun  from keyspace_user02.data;";
    ResultSet resultSet = session.execute(cql);
    Row r = resultSet.one();
    len = r.getLong("coun");
    System.out.println(r.getLong("coun"));
  }
  public long start() {
    threads = new Thread[threadNum];
    long begin = System.currentTimeMillis();
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(this);
      threads[i].setName(String.valueOf(i));
      threads[i].start();
    }
    long end = 0;
    try {
      countDownLatch.await();
      end = System.currentTimeMillis();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      if (fsi != null) {
        try {
          fsi.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if (bo != null) {
        try {
          bo.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    System.out.println("begin " + begin + " end " + end + " cost " + (end - begin));
    return end - begin;
  }
  public synchronized HashSet<Integer> nextRecord() {
    if (index < len) {
      String cql = "select * from keyspace_user02.data where i=" + index + " ;";
      ResultSet resultSet = session.execute(cql);
      Row row = resultSet.one();
      index++;
      System.out.println(row.getInt("i")+" "+"index: "+index+" "+Integer.valueOf(Thread.currentThread().getName()));
      return (HashSet<Integer>) row.getSet("data", Integer.class);
    }
    return null;
  }
  public void compare() {
    int[] num = new int[] { 1, 2, 4, 8, 16, 32 };// 1, 2, 4, 8, 16, 32
    Map<Integer, Long> cost = new HashMap<Integer, Long>();
    System.out.println(method);
    for (int i = 0; i < num.length; i++) {
      try {
        this.set("data.txt", method + "_ThreadNum_" + num[i] , num[i]);
        long time = this.start();
        cost.put(num[i], time);
        System.out.println(num[i] + ":" + cost.get(num[i]));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
  public static void main(String[] args) {
    new CAWriter1("CAWriter1").compare();
  }
}

class CAWriter1 extends CAWriter {
  int index = 0;
  public CAWriter1(String method) {
    super(method);
  }
  public void run() {
    int id = Integer.valueOf(Thread.currentThread().getName());

    try {
      destFileLock.lock();
      while (written != id) {
        System.out.println(id + " is waiting " );
          writeCondition.await();
      }
      HashSet<Integer> set = this.nextRecord();
      Insert insert = QueryBuilder.insertInto("keyspace_user02", dest_file).value("i", index).value("data", set);
      System.out.println(id + " is writing to " + dest_file + ":" + index);
      ResultSet result = session.execute(insert.toString());
      System.out.println(result);
      written++;
      writeCondition.signalAll();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      destFileLock.unlock();
      countDownLatch.countDown();
    }

  }
  
}