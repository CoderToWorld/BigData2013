package com.atguigu.mr.wordcount;

/**
 * @author suyso
 * @create 2020-04-14 18:40
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 自定义Reducer，需要继承Reducer类，指定4个泛型，4个泛型表示两组kv对
 * 4个泛型，两组kv对，一组输入kv，一组输出kv
 *
 * 输入的kv对 ：
 * KEYIN : Text，对应Map输出的k，就是一个单词
 * VALUEIN : IntWritable ，对应Map输出的v ，表示单词出现的次数
 *
 * 输出的kv对 ：
 * KEYOUT : Text ，表示一个单词
 * VALUEOUT :IntWritable，表示这个单词出现的总次数
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    //定义写出的v
    IntWritable outV = new IntWritable();


    /**
     ** 重写Reducer中的reduce方法
     *
     * @param key     表示输入的k ，就是一个单词
     * @param values  表示封装了当前的key对应的所有value的一个迭代对象
     * @param context 负责调度Reducer
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //迭代values ，将当前key对应的所有value累加
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        //写出
        //将累加的结果sum封装到outV中
        outV.set(sum);
        context.write(key,outV);
    }
}
