package com.atguigu.hive.etl;

/**
 * @author suyso
 * @create 2020-04-29 11:16
 */
public class EtlUtils {
    /**
     * 清洗video的数据
     * 清洗规则：
     * 1.
     */
    public static String etlData(String srcData) {
        StringBuilder sbs = new StringBuilder();
        //通过分隔符\t切割数据
        String[] datas = srcData.split("\t");
        //1.判断数据是否完整
        if (datas.length < 9) {
            return null;
        }
        //2.处理视频类别的空格
        datas[3] = datas[3].replaceAll(" ", "");

        //3.处理相关视频
        for (int i = 0; i < datas.length; i++) {
            if (i < 9){
                //思考没有相关视频的情况
                if(i < datas.length -1){
                    sbs.append(datas[i]).append("\t");
                }else{
                    sbs.append(datas[i]);
                }
            }else{
                //思考有相关视频的情况
                if(i < datas[i].length() - 1){
                    sbs.append(datas[i]).append("&");
                }else{
                    sbs.append(datas[i]);
                }
            }

        }

        return sbs.toString();
    }

    public static void main(String[] args) {
        String result = etlData("fQShwYqGqsw	lonelygirl15	736	People & Blogs	133	151763	3.01	666	765	fQShwYqGqsw	LfAaY1p_2Is	5LELNIVyMqo	vW6ZpqXjCE4	vPUAf43vc-Q	ZllfQZCc2_M	it2d7LaU_TA	KGRx8TgZEeU	aQWdqI1vd6o	kzwa8NBlUeo	X3ctuFCCF5k	Ble9N2kDiGc	R24FONE2CDs	IAY5q60CmYY	mUd0hcEnHiU	6OUcp6UJ2bA	dv0Y_uoHrLc	8YoxhsUMlgA	h59nXANN-oo	113yn3sv0eo"
);
        System.out.println(result);
    }
}
