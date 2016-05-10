package ClassParse;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by LJian on 2016/4/19.
 */
public class ClassParseTest {
    @Test
    public void testObj2Json(){
        InnerClass ic = new InnerClass();
        ic.setDoubleS(1.1);
        ic.setIntS(1);
        ic.setStringS("test string");
        List<Double> listI = new ArrayList<>();
        listI.add(12.2);
        listI.add(13.3);
        listI.add(14.4);
        listI.add(15.5);
        listI.add(16.6);
        ic.setListS(listI);

        Map<Integer, String> mapI = new HashMap<>();
        mapI.put(11, "one");
        mapI.put(12, "two");
        mapI.put(13, "three");
        mapI.put(14, "four");
        ic.setMapS(mapI);


        SampleClass sc = new SampleClass();
        sc.setDoubleS(1.1);
        sc.setIntS(1);
        sc.setStringS("test string");
        List<Double> list = new ArrayList<>();
        list.add(2.2);
        list.add(3.3);
        list.add(4.4);
        list.add(5.5);
        list.add(6.6);
        sc.setListS(list);

        Map<Integer, String> map = new HashMap<>();
        map.put(1, "one");
        map.put(2, "two");
        map.put(3, "three");
        map.put(4, "four");
        sc.setMapS(map);

        sc.setInnerClass(ic);

        ClassParse cp = new ClassParse();
        String res = cp.obj2Json(sc);
        System.out.println(res);


    }
}
