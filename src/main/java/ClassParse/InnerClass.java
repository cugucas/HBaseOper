package ClassParse;

import java.util.List;
import java.util.Map;

/**
 * Created by LJian on 2016/4/19.
 */
public class InnerClass {
    private Integer intS;
    private Double doubleS;
    private String stringS;
    private Map<Integer, String> mapS;
    private List<Double> listS;

    public InnerClass() {

    }

    public Integer getIntS() {
        return intS;
    }

    public void setIntS(Integer intS) {
        this.intS = intS;
    }

    public String getStringS() {
        return stringS;
    }

    public void setStringS(String stringS) {
        this.stringS = stringS;
    }

    public Double getDoubleS() {
        return doubleS;
    }

    public void setDoubleS(Double doubleS) {
        this.doubleS = doubleS;
    }

    public Map<Integer, String> getMapS() {
        return mapS;
    }

    public void setMapS(Map<Integer, String> mapS) {
        this.mapS = mapS;
    }

    public List<Double> getListS() {
        return listS;
    }

    public void setListS(List<Double> listS) {
        this.listS = listS;
    }
}
