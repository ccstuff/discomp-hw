import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public abstract class MTWriter implements Runnable {
  String method = "MTWriter";
  String source_file = "data.txt";
  String dest_file = "mtwriter.txt";
  int threadNum;
  Thread threads[];
  CountDownLatch countDownLatch;
  int bufSize = 1024 * 1024;

  public MTWriter(String method) {
    this.method = method;
  }

  public void set(String source_file, String dest_file, int num) {
    this.source_file = source_file;
    this.dest_file = dest_file;
    this.threadNum = num;
    this.countDownLatch = new CountDownLatch(num);
  }

  public abstract void run();

  public long start() {
    threads = new Thread[threadNum];
    long begin = System.currentTimeMillis();
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(this, i + "");
      threads[i].start();
    }
    long end = 0;
    try {
      countDownLatch.await();
      end = System.currentTimeMillis();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    System.out.println("begin " + begin + ", end " + end + " cost " + (end - begin));
    return end - begin;
  }

  public void compare() {
    int[] num = new int[] { 1, 2, 4, 8, 16, 32 };
    Map<Integer, Long> cost = new HashMap<Integer, Long>();
    System.out.println(method);
    for (int i = 0; i < num.length; i++) {
      try {
        this.set("data.txt", method + "_ThreadNum_" + num[i] + ".txt", num[i]);
        long time = this.start();
        cost.put(num[i], time);
        System.out.println("线程数" + num[i] + ":" + cost.get(num[i]));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) throws IOException {
    new MTWriterWithMappedByteBuffer("MTWriterWithMappedByteBuffer").compare();
    new MTWriterByTransferTo("MTWriterByTransferTo").compare();
    new MTWriterWithTwoRaf("MTWriterWithTwoRaf").compare();
    new MTWriterWithRaf("MTWriterWithRaf").compare();
    System.out.println("end");
  }
}

class MTWriterWithMappedByteBuffer extends MTWriter {
  public MTWriterWithMappedByteBuffer(String method) {
    super(method);
  }

  public void run() {
    int id = Integer.valueOf(Thread.currentThread().getName());
    RandomAccessFile rafi = null;
    RandomAccessFile rafo = null;
    FileChannel fci = null;
    FileChannel fco = null;
    try {
      rafi = new RandomAccessFile(source_file, "r");
      rafo = new RandomAccessFile(dest_file, "rw");
      fco = rafo.getChannel();
      fci = rafi.getChannel();
      long length = rafi.length() % threadNum == 0 ? rafi.length() / threadNum
          : ((int) (Math.ceil((double) rafi.length() / threadNum)));// 计算每个线程应该读取的长度
      long begin = (int) (id * length);

      MappedByteBuffer mbbi = fci.map(FileChannel.MapMode.READ_ONLY, begin, length);
      synchronized (this) {
        MappedByteBuffer mbbo = fco.map(FileChannel.MapMode.READ_WRITE, begin, length);
        mbbo.put(mbbi);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (rafi != null)
        try {
          rafi.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      if (rafo != null)
        try {
          rafo.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      countDownLatch.countDown();
    }
  }
}

class MTWriterByTransferTo extends MTWriter {
  public MTWriterByTransferTo(String method) {
    super(method);
  }

  public void run() {
    FileInputStream fis = null;
    FileOutputStream fos = null;
    FileChannel fisChannel = null;
    FileChannel fosChannel = null;
    try {
      fis = new FileInputStream(source_file);
      fos = new FileOutputStream(dest_file);
      fisChannel = fis.getChannel();
      fosChannel = fos.getChannel();
      long len = fisChannel.transferTo(0, fisChannel.size(), fosChannel);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (fisChannel != null) {
        try {
          fisChannel.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if (fis != null) {
        try {
          fis.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if (fosChannel != null) {
        try {
          fosChannel.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if (fos != null) {
        try {
          fos.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public void compare() {
    System.out.println(method);
    long startTime = System.currentTimeMillis();
    try {
      this.set("data.txt", method + "_ThreadNum_" + 1 + ".txt", 1);
      new Thread(this).start();
    } catch (Exception e) {
      e.printStackTrace();
    }
    long endTime = System.currentTimeMillis();
  }
}

class MTWriterWithTwoRaf extends MTWriter {
  public MTWriterWithTwoRaf(String method) {
    super(method);
  }

  public void run() {
    RandomAccessFile reader = null;
    RandomAccessFile writer = null;
    try {
      reader = new RandomAccessFile(source_file, "r");//
      writer = new RandomAccessFile(dest_file, "rw");
      long length = reader.length() % threadNum == 0 ? reader.length() / threadNum
          : ((int) (Math.ceil((double) reader.length() / threadNum)));// 计算每个线程应该读取的长度
      int id = Integer.valueOf(Thread.currentThread().getName());
      long begin = id * length;
      long end = (begin + length) > reader.length() ? reader.length() : (begin + length);

      reader.seek(begin);
      writer.seek(begin);

      int len = 0;
      byte buffer[] = new byte[bufSize];
      while (begin < end) {
        if (begin + bufSize < end) {
          len = reader.read(buffer);
        } else {
          len = reader.read(buffer, 0, (int) (end - begin));
        }
        writer.write(buffer, 0, len);
        begin += len;
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (reader != null)
        try {
          reader.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      if (writer != null)
        try {
          writer.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      countDownLatch.countDown();
    }
  }
}

class MTWriterWithRaf extends MTWriter {
  public MTWriterWithRaf(String method) {
    super(method);
  }

  public void run() {
    RandomAccessFile reader = null;
    RandomAccessFile writer = null;
    try {
      reader = new RandomAccessFile(source_file, "r");
      writer = new RandomAccessFile(dest_file, "rw");
      FileChannel finChannel = reader.getChannel();

      long length = reader.length() % threadNum == 0 ? reader.length() / threadNum
          : ((int) (Math.ceil((double) reader.length() / threadNum)));// 计算每个线程应该读取的长度
      int id = Integer.valueOf(Thread.currentThread().getName());
      long begin = (int) (id * length);

      ByteBuffer inBuffer = finChannel.map(FileChannel.MapMode.READ_ONLY, begin, length);
      writer.seek(begin);
      try {
        long end = begin + length;
        while (begin < end) {
          byte[] buf = new byte[bufSize];
          if (begin + bufSize <= end) {
            inBuffer.get(buf);
            begin += bufSize;
            writer.write(buf);
          } else {
            inBuffer.get(buf, 0, (int) (end - begin));
            writer.write(buf, 0, (int) (end - begin));
            begin += (end - begin);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (reader != null)
        try {
          reader.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      if (writer != null)
        try {
          writer.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      countDownLatch.countDown();
    }
  }
}
