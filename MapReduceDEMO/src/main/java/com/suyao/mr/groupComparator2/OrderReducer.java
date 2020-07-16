package com.suyao.mr.groupComparator2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<OrderBean, NullWritable,OrderBean,NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //思考: 假如在一个订单中的最高金额有相同的多个商品.


        // 进入到一个reduce方法的key，订单id一定都是一样的，且价格是倒序排序好的。

        //直接将第一个数据写出即可.
        context.write(key,NullWritable.get());

        //context.write(key,values.iterator().next());
    }
}
