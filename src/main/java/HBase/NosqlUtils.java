package HBase;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by LJian on 2016/5/16.
 */
public class NosqlUtils {
    public static String getCurrentDateTime(){
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        return dateFormat.format(now);
    }

    public static String getQueryString(String time, String user, String type){
        StringBuffer query = new StringBuffer();
        if(null != time && !time.isEmpty()){
            query.append(time);
        }else{
            query.append("00000000000000");
        }

        if(null != user && !user.isEmpty()){
            query.append(user);
        }else{
            query.append("0000");
        }

        if(null != type && !type.isEmpty()){
            query.append(type);
        }else{
            query.append("0000");
        }

        return query.toString();
    }
}
