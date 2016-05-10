package ClassParse;

import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by LJian on 2016/4/11.
 */
public class ClassParse {

    public String obj2Json(Object obj){
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
}
