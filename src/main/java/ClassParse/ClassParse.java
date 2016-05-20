package ClassParse;

import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by LJian on 2016/4/11.
 */
public class ClassParse {

    public static String obj2Json(Object obj){
        if(null == obj){
            return null;
        }
        ObjectMapper mapper = new ObjectMapper();
        OutputStream out = new ByteArrayOutputStream();
        try {
            mapper.writeValue(out, obj);
        } catch (IOException e) {
            e.printStackTrace();
        }

        String result = out.toString();
        return result;
    }

    public static Object json2Obj(String jsonString, Class clazz){
        ObjectMapper mapper = new ObjectMapper();
        Object obj = null;
        try {
            obj  = mapper.readValue(jsonString, clazz);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return obj;
    }
}
