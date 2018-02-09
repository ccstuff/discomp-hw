import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.Bytes;
import com.sun.tools.classfile.Opcode.Set;

import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.List;

public class Producer {
  private static int times = 256;
  private static int number = 2014 * 512;

  public long produce(String file) throws IOException {
    long startTime = System.currentTimeMillis();
    RandomAccessFile raf = null;
    FileChannel fc = null;
    try {
      raf = new RandomAccessFile(file, "rw");
      fc = raf.getChannel();
      IntBuffer ib = fc.map(FileChannel.MapMode.READ_WRITE, 0, number * times * 4).asIntBuffer();
      for (int i = 1; i <= number; i++) {
        for (int j = 0; j < times; j++) {
          ib.put(i);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (fc != null) {
        fc.close();
      }
      if (raf != null) {
        raf.close();
      }
    }
    long endTime = System.currentTimeMillis();
    System.out.println("start time:" + startTime + ", end time:" + endTime + ", cost: " + (endTime - startTime));
    return endTime - startTime;
  }

  public long produceOnHdfs() throws IOException {
    long startTime = System.currentTimeMillis();
    HDWriter hdWriter = new HDWriter1("");
    FSDataOutputStream fso = null;
    try {
      fso = hdWriter.getOutputStream("data.txt");
      BufferedOutputStream bo = new BufferedOutputStream(fso);
      ByteBuffer buf = ByteBuffer.allocate(times * 4);
      for (int i = 1; i <= number; i++) {
        for (int j = 0; j < times; j++) {
          buf.putInt(i);
        }
        // byte[] b = new byte[times * 4];
        // buf.get(b);
        bo.write(buf.array());
        buf.clear();
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (fso != null) {
        fso.close();
      }
    }
    long endTime = System.currentTimeMillis();
    System.out.println("start time:" + startTime + ", end time:" + endTime + ", cost: " + (endTime - startTime));
    return endTime - startTime;
  }

  public long produceOnCassandra() throws IOException {
    long startTime = System.currentTimeMillis();
    Session session = null;
    try {
      session = new CAWriter1("").getSession();
      String query = "DROP TABLE data;";
      ResultSet rs = session.execute(query);
      System.out.println(rs);
      String createTableCQL = "create table if not exists keyspace_user02.data(i int PRIMARY KEY, data set<int>)";
      session.execute(createTableCQL);
      System.out.println("begin produce");
      ByteBuffer buf = ByteBuffer.allocate(times * 4);
      int index = 0;
      for (int i = 1; i <= number; i++) {
        System.out.print(i);
        HashSet<Integer> set = new HashSet<Integer>(256);
        for (int k = 0; k < 20000; k++) {
          for (int j = 0; j < times; j++) {
            set.add(i);
          }
          i++;
        }
        Insert insert = QueryBuilder.insertInto("keyspace_user02", "data").value("i", index++).value("data", set);
        ResultSet result = session.execute(insert.toString());
        System.out.println(result);
        buf.clear();
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (session != null) {
        session.close();
      }
    }
    long endTime = System.currentTimeMillis();
    System.out.println("start time:" + startTime + ", end time:" + endTime + ", cost: " + (endTime - startTime));
    return endTime - startTime;
  }

  public static void main(String[] args) throws IOException {
    new Producer().produce("data.txt");
    new Producer().produceOnHdfs();
    new Producer().produceOnCassandra();
  }
}