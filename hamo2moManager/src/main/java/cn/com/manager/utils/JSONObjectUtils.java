package cn.com.manager.utils;

import cn.com.manager.exception.JSONDataException;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;

/**
 * @Author huwenhu
 * @Date 2019/8/28 15:54
 **/

public class JSONObjectUtils {

    public static boolean isJSONData(String message){
        if(StringUtils.isBlank(message)){
            return false;
        }
        try {
            JSONObject.parseObject( message );
        } catch (Exception e){
            return false;
        }
        return true;
    }


    public static void main(String[] args){
        System.out.println(isJSONData( "123" ));
    }

}
