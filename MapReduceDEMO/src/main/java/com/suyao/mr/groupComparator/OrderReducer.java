package com.suyao.mr.groupComparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author suyso
 * @create 2020-04-17 21:32
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable,OrderBean,NullWritable> {
    //思考: 假如在一个订单中的最高金额有相同的多个商品.


    // 进入到一个reduce方法的key，订单id一定都是一样的，且价格是倒序排序好的。

    //直接将第一个数据写出即可.
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
