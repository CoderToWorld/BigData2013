package com.suyao.mr.mapjoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author suyso
 * @create 2020-04-19 16:13
 */

/**
 * MapJoin的思想：
 * 将小表的文件提前加载到内存中，接下来每读取一条大表的数据，就与内存中的小表的数据进行join，join完成以后直接写出
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Map<String, String> pdMap = new HashMap<>();

    private Text outK = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //将小表的数据加载到内存中
        //获取在driver中设置的缓存文件
        URI[] cacheFiles = context.getCacheFiles();//获取缓存文件
        URI currentCacheFile = cacheFiles[0];//获取缓存文件数组的第一个文件

        //读取文件
        FileSystem fs = FileSystem.get(context.getConfiguration());
        //获取输入流
        FSDataInputStream in = fs.open(new Path(currentCacheFile.getPath()));
        //一次读取文件中的一行数据
        BufferedReader reader = new BufferedReader((new InputStreamReader(in)));
        String line;
        while ((line = reader.readLine()) != null) {
            //切割
            //01 小米
            String[] split = line.split("\t");
            pdMap.put(split[0], split[1]);

        }
        //pdMap : 01 - 小米   02 - 华为  03 - 格力

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //计数器
        context.getCounter("Map Join", "map").increment(1);

        //读取大表文件
        String line = value.toString();
        //切割
        //1001 01 1
        String[] split = line.split("\t");
        String currentPname = pdMap.get(split[1]);
        String resultLine = split[0] + "\t" + currentPname + "\t" + split[2];

        //封装key
        outK.set(resultLine);

        //写出
        context.write(outK, NullWritable.get());
    }
}
