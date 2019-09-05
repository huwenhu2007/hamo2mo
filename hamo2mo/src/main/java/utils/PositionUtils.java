package utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import exception.ZKException;
import oplog.MongoConfig;
import oplog.entity.OplogEvent;
import oplog.entity.OplogPosition;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author huwenhu
 * @Date 2019/7/12 17:12
 **/
public class PositionUtils {

    private static Logger logger = Logger.getLogger(PositionUtils.class);

    public static void main(String[] args) {
        Date time = new Date();

        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(time.getTime());

        int time1 = 0;
        try {
            time1 = (int)(sdf.parse( "2019-08-22 16:38:56" ).getTime() / 1000);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        System.out.println(time1);

        System.out.println(sdf.format(new Long(1510049394) * 1000));
    }

    public static OplogPosition findDefaultOplogPosition(MongoConfig mongoConfig) throws IOException {
        OplogPosition oplogPosition=new OplogPosition();
        oplogPosition.setStrIP(mongoConfig.getStrIP());
        oplogPosition.setNPort(mongoConfig.getNPort());
        oplogPosition.setStrSign( "localtime" );
        oplogPosition.setStrWorkId(mongoConfig.getStrWorkerId());

        long lTime = System.currentTimeMillis() / 1000L;
        try {
            oplogPosition.setNTime((int)lTime);
        } catch (Exception e) {
            logger.warn("findDefaultOplogPosition strOplogPosition  lTime to int "+lTime+" ",e);
            return null;
        }
        oplogPosition.setNIncrement(1);
        return	oplogPosition;
    }

    /**
     * 获取可用位置信息
     * @param mongoConfig
     * @return
     * @throws IOException
     */
    public static OplogPosition findBinlogPositionList(MongoConfig mongoConfig) throws IOException {
        OplogPosition  position=null;

        if(1 == mongoConfig.getNPositionEnable()){
            position=new OplogPosition(mongoConfig.getStrIP(), mongoConfig.getNPort(), mongoConfig.getStrWorkerId(), "config", mongoConfig.getNTime(), mongoConfig.getNIncrement());
            return position;
        }

        JSONObject pJSONObject = mongoConfig.getZkPosition();
        if(Objects.isNull( pJSONObject )){
            position = PositionUtils.findDefaultOplogPosition(mongoConfig);
        } else {
            int time = pJSONObject.getInteger( "time" );
            int increment = pJSONObject.getInteger( "increment" );
            position = new OplogPosition( mongoConfig.getStrIP(), mongoConfig.getNPort(), mongoConfig.getStrWorkerId(), "zk", time, increment );
        }

        return position;
    }

    public static JSONObject getPositionFromZK(JSONObject jsonObject) throws ZKException{
        try {
            JSONObject t = jsonObject.getJSONObject( "t" );
            Set<String> keySet = t.keySet();
            Iterator<String> iterator = keySet.iterator();
            int fetchTime = Integer.MAX_VALUE;
            int fetchIcre = Integer.MAX_VALUE;

            SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
            while (iterator.hasNext()) {
                String p = iterator.next();
                String pData = t.getString( p );
                if(StringUtils.isBlank( pData )){
                    continue;
                }

                String[] pArr = pData.split( "\\|" );
                String time = pArr[0];
                int time1 = (int) (sdf.parse( time ).getTime() / 1000);
                String icre = pArr[1];
                int icre1 = Integer.valueOf( icre );
                if (fetchTime > time1) {
                    fetchTime = time1;
                    fetchIcre = icre1;
                } else if (fetchTime == time1 && fetchIcre > icre1) {
                    fetchIcre = icre1;
                }
            }

            JSONObject pJSONObject = new JSONObject();
            pJSONObject.put( "time", fetchTime );
            pJSONObject.put( "increment", fetchIcre );
            return pJSONObject;
        } catch(Exception e){
            throw new ZKException( e );
        }
    }
}
