package com.atguigu.hdfs;

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.xml.xpath.XPath;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;


/**
 * @author suyso
 * @create 2020-04-13 9:21
 */
public class TestHDFS {

    private FileSystem fs;
    private Configuration conf;

    //在@Test方法执行前执行
    @Before
    public void before() throws IOException, InterruptedException {
        URI uri = URI.create("hdfs://hadoop103:9820");
        conf = new Configuration();
        String user = "atguigu";

        fs = FileSystem.get(uri, conf, user);
    }

    //在@Test方法执行之后执行
    @After
    public void after() throws IOException {
        fs.close();
    }

    /**
     * 在HDFS上创建目录
     */
    @Test
    public void testMkdir() throws IOException, InterruptedException {
        //1.获取客户端对象，文件系统对象
        URI uri = URI.create("hdfs://hadoop103:9820");
        Configuration conf = new Configuration();
        String user = "atguigu";
        FileSystem fileSystem = FileSystem.get(uri, conf, user);

        //2.操作
        fileSystem.mkdirs(new Path("/test2"));

        //3.关闭资源
        fileSystem.close();


    }

    /**
     * 文件的上传
     * <p>
     * 配置的优先级：
     * <p>
     * 代码中的Configuration > Module 中的xxx-site.xml > 集群中的xxx-site.xml > 集群中的xxx-site.xml
     */
    @Test
    public void testUpLoad() throws IOException, InterruptedException {
        URI uri = URI.create("hdfs://hadoop103:9820");
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        String user = "atguigu";

        FileSystem fs = FileSystem.get(uri, conf, user);
        fs.copyFromLocalFile(false, true,
                new Path("d:/hello.txt"),
                new Path("/test2"));
        fs.close();
    }

    /**
     * 文件的下载
     */
    @Test
    public void testDownLoad() throws IOException {
        fs.copyToLocalFile(false,
                new Path("/test2/nihao.txt"),
                new Path("d:/hadoop/nihao1.txt"),
                false);
    }

    /**
     * 删除文件，如果是空目录或者是具体的文件，可以直接删除，不需要将参数设置为true或false
     * 如果是非空目录，需要删除的话，需要设置为true
     *
     * @throws IOException
     */
    @Test
    public void testDelete() throws IOException {
        //删除文件
        //fs.delete(new Path("/test2/hello.txt"),true);
        //删除目录
        fs.delete(new Path("/test2"), true);
    }

    /**
     * 文件的更名和移动
     */
    @Test
    public void testRename() throws IOException {
        //文件更名
        // fs.rename(new Path("/testHDFS/hello.txt"),new Path("/testHDFS/HELLO.txt"));
        //文件移动
        fs.rename(new Path("/testHDFS/HELLO.txt"),
                new Path("/hello.txt"));

    }

    /**
     * 文件的详情查看
     */
    @Test
    public void testListFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            //去下一个
            LocatedFileStatus next = listFiles.next();
            //每个文件的详情
            System.out.println(next.getPermission() + "\t"
                    + next.getOwner() + "\t"
                    + next.getGroup() + "\t"
                    + next.getLen() + "\t"
                    + next.getReplication() + "\t"
                    + next.getBlockSize() + "\t"
                    + next.getPath().getName());
            //获取当前文件的块信息
            BlockLocation[] blockLocations = next.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
            System.out.println("======================");


        }
    }
    /**
     * 文件和文件的判断
     */
    @Test
    public void testDirOrFile() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for(FileStatus status : listStatus){
            if(status.isDirectory()){
                System.out.println(status.getPath().getName() + "是一个文件");
            }else{
                System.out.println(status.getPath().getName() + "是一个目录");
            }
        }
    }

    /**
     * 递归显示HDFS中执行目录下的所有子目录和文件
     */

    public void printAllDirAndFiles(String path,FileSystem fs) throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path(path));
        for (FileStatus status : listStatus) {
            if (!status.isDirectory()) {
                System.out.println("File-->" + status.getPath().toString());
            } else {
                System.out.println("Dir-->" + status.getPath());
                printAllDirAndFiles(status.getPath().toString(),fs);
            }
        }
    }
    @Test
    public void testprintAllDirAndFiles() throws IOException {
        printAllDirAndFiles("/",fs);

    }

    /**
     * 利用IO流上传文件
     */
    @Test
    public void testIOUpLoad() throws IOException {
        String src = "d:/hello.txt";
        String dest = "/hello.txt";

        FileInputStream in = new FileInputStream(new File(src));
        FSDataOutputStream out = fs.create(new Path(dest));

        IOUtils.copyBytes(in,out,conf);

        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }
    /**
     * 利用IO流下载文件
     */
    @Test
    public void testIODownLoad() throws IOException {
        String src = "/hello.txt";
        String dest = "d:/hadoop/hello.txt";
        FSDataInputStream in = fs.open(new Path(src));
        FileOutputStream out = new FileOutputStream(new File(dest));
        IOUtils.copyBytes(in,out,conf);
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);


    }

}
