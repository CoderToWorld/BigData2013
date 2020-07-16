package com.atguigu.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @author suyso
 * @create 2020-04-28 14:16
 */

/**
 * 需求：计算传入的字符串长度并返回
 * <p>
 * 自定义UDF,需要继承GenericUDF类
 */
public class MyUDF extends GenericUDF {
    /**
     * 对输入参数的判断处理和返回值类型的一个约定
     *
     * @param objectInspectors 传入到函数的参数的类型对应的objectInspector
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors == null || objectInspectors.length != 1) {
            throw new UDFArgumentLengthException("输入参数长度错误！！！");
        }
        if (!objectInspectors[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
            throw new UDFArgumentTypeException(0, "输入参数类型错误！！！");
        }
        //约定函数的返回值类型为int
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * 函数的逻辑处理
     *
     * @param deferredObjects 传入到函数的参数
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        //取出参数
        Object o = deferredObjects[0].get();
        if (o == null) {
            return 0;
        }
        return o.toString().length();
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
