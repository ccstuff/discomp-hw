import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import static org.apache.hadoop.io.IOUtils.copyBytes;

public abstract class HDWriter implements Runnable {
  final String hdfsUrl = "hdfs://148.100.92.156:4000/user/user02/";
  FileSystem fs;
  Configuration configuration;
  String keyFs = "fs.defaultFS";
  String method = "HDWriter";
  String source_file = "data.txt";
  String dest_file = "mtwriter.txt";
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

  public HDWriter(String method) {
    this.method = method;
    destFileLock = new ReentrantLock();
    writeCondition = destFileLock.newCondition();
    try {
      UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("accnm"));
      configuration = new Configuration();
      configuration.set(keyFs, hdfsUrl);
      configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
      fs = FileSystem.get(new URI(hdfsUrl), configuration);
    } catch (Exception e) {
      System.out.println("hdfs connect fail");
      e.printStackTrace();
    }
  }

  public void set(String source_file, String dest_file, int num) {
    this.source_file = source_file;
    this.dest_file = dest_file;
    this.threadNum = num;
    this.countDownLatch = new CountDownLatch(num);
    this.createFile(dest_file);
    this.written = 0;
    try {
      bo = new BufferedOutputStream(fs.append(new Path(hdfsUrl + dest_file)));
      fsi = fs.open(new Path(hdfsUrl + source_file));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public abstract void run();

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

  public void compare() {
    int[] num = new int[] { 1, 2, 4, 8, 16, 32 };// 1, 2, 4, 8, 16, 32
    Map<Integer, Long> cost = new HashMap<Integer, Long>();
    System.out.println(method);
    for (int i = 0; i < num.length; i++) {
      try {
        this.set("data.txt", method + "_ThreadNum_" + num[i] + ".txt", num[i]);
        long time = this.start();
        cost.put(num[i], time);
        System.out.println(num[i] + ":" + cost.get(num[i]));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public FSDataOutputStream getOutputStream(String filename) throws IOException, URISyntaxException {
    Path file = null;
    FSDataOutputStream outputStream = null;
    try {
      file = new Path(hdfsUrl + filename);
      if (fs.exists(file)) {
        fs.delete(file, true);
      }
      outputStream = fs.create(file, (short) 1);
      System.out.println("Get FSDataOutputStream");
    } catch (Exception e) {
      System.out.println("get FSDataOutputStream fail");
      e.printStackTrace();
    }
    return outputStream;
  }

  public void createFile(String dest_file) {
    Path file = null;
    FSDataOutputStream outputStream = null;
    try {
      file = new Path(hdfsUrl + dest_file);
      if (fs.exists(file)) {
        fs.delete(file, true);
      }
      outputStream = fs.create(file, (short) 1);
      System.out.println("create " + dest_file + " sccesss");
    } catch (Exception e) {
      System.out.println("create " + dest_file + " fail");
      e.printStackTrace();
    } finally {
      if (outputStream != null) {
        try {
          outputStream.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public long getFileSize(String file) throws IOException, FileNotFoundException {
    long length = -1;
    try {
      FileStatus stat = fs.getFileStatus(new Path(hdfsUrl + file));
      length = stat.getLen();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return length;
  }

  public static void main(String[] args) throws IOException, URISyntaxException {
    new HDWriter1("HDWriter1").compare();
    new HDWriter2("HDWriter2").compare();
    System.out.println("end");
  }
}

class HDWriter1 extends HDWriter {
  public HDWriter1(String method) {
    super(method);
  }

  public void run() {
    int id = Integer.valueOf(Thread.currentThread().getName());
    try {
      long size = this.getFileSize(source_file);
      long length = size % threadNum == 0 ? size / threadNum : ((int) (Math.ceil((double) size / threadNum)));// 计算每个线程应该读取的长度
      long begin = id * length;

      destFileLock.lock();
      while (written != id) {
        writeCondition.await();
      }
      System.out.println(threadNum + ": thread " + id + " writing");
      fsi.seek(begin);
      copyBytes(fsi, bo, length, false);
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

class HDWriter2 extends HDWriter {
  public HDWriter2(String method) {
    super(method);
  }

  public void run() {
    int id = Integer.valueOf(Thread.currentThread().getName());
    try {
      long size = this.getFileSize(source_file);
      long length = size % threadNum == 0 ? size / threadNum : ((int) (Math.ceil((double) size / threadNum)));// 计算每个线程应该读取的长度
      long begin = id * length;
      long end = (begin + length) > size ? size : (begin + length);

      destFileLock.lock();
      while (written != id) {
        writeCondition.await();
      }
      System.out.println(threadNum + ": thread " + id + " writing");
      fsi.seek(begin);
      int len = 0;
      byte buffer[] = new byte[bufSize];
      while (begin < end) {
        if (begin + bufSize < end) {
          len = fsi.read(buffer);
        } else {
          len = fsi.read(buffer, 0, (int) (end - begin));
        }
        bo.write(buffer, 0, len);
        begin += len;
      }
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