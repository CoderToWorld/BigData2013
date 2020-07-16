package com.suyao.mr.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author suyso
 * @create 2020-04-19 14:20
 */
public class ReduceJoinReducer extends Reducer<Text, OrderBean, OrderBean, NullWritable> {
    //定义存储Order数据的OrderBean集合
    List<OrderBean> orders = new ArrayList<>();

    //定义OrderBean,存储pd的数据
    OrderBean pdBean = new OrderBean();

    @Override
    protected void reduce(Text key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
        //pid相同的数据会进入到一个reduce方法
        //  1001	01	1
        //  1004	01	4
        //  01	小米
        /*
            思路：将所有order的数据全部获取到，保存到一个集合中，把pd的数据获取到，保存到一个对象中
            迭代保存order数据的集合，获取到每个每个order数据的OederBean对象，把pd对象中的pname设置到每
            个order数据的OrderBean对象中

         */
        for (OrderBean orderBean : values) {
            if ("order".equals(orderBean.getFlag())) {
                //order数据
                //深拷贝
                OrderBean currentOrderBean = new OrderBean();
                try {
                    BeanUtils.copyProperties(currentOrderBean, orderBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orders.add(currentOrderBean);
            } else {
                //pd数据
                try {
                    BeanUtils.copyProperties(pdBean, orderBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }

            }
        }
        for (OrderBean orderBean : orders) {
            //给OrderBean的pname赋值
            orderBean.setPname(pdBean.getPname());
            context.write(orderBean, NullWritable.get());
        }
        //清空集合
        orders.clear();
    }
}