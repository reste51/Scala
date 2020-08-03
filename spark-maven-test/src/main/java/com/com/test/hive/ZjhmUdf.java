package com.com.test.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/** 自定义  hive 的 user defined  function
 * @ClassName TOMUDF
 * @Description TODO
 * @Author 50204
 * @Date 2020/7/6 22:04
 * @Version 1.0
 **/
public final class ZjhmUdf  extends UDF{

    public Text evaluate(final Text text){
        String str = "";
        if(text == null) str = "";
        else str = text.toString();

        String[] input = str.split("");

        String[] result = new String[18];
        for(int i=0;i<input.length;i++){
            if(i<=5){
                result[i] = input[i];
            }else{
                result[i+2] = input[i];
            }
        }
        //年份最后两位小于17,年份为20XX，否则为19XX
        if(Integer.valueOf(input[6])<=1&&Integer.valueOf(input[7])<=7){
            result[6]="2";
            result[7]="0";
        }else{
            result[6]="1";
            result[7]="9";
        }
        //计算最后一位
        String[] xs = {"7","9","10","5","8","4","2","1","6","3","7","9","10","5","8","4","2"};
        //前十七位乘以系数[7,9,10,5,8,4,2,1,6,3,7,9,10,5,8,4,2],
        int sum = 0;
        for(int i=0;i<17;i++){
            sum+= Integer.valueOf(result[i]) * Integer.valueOf(xs[i]);
        }
        //对11求余，的余数 0 - 10
        int rod = sum % 11;
        //所得余数映射到对应数字即可
        if(rod==0){ result[17] = "1";
        }else if(rod==1){ result[17] = "0";
        }else if(rod==2){ result[17] = "X";
        }else if(rod==3){ result[17] = "9";
        }else if(rod==4){ result[17] = "8";
        }else if(rod==5){ result[17] = "7";
        }else if(rod==6){ result[17] = "6";
        }else if(rod==7){ result[17] = "5";
        }else if(rod==8){ result[17] = "4";
        }else if(rod==9){ result[17] = "3";
        }else if(rod==10){ result[17] = "2";}



        return  new Text(StringUtils.join(result));
    }
}
