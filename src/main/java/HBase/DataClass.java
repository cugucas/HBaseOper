package HBase;

import java.util.Map;

/**
 * Created by LJian on 2016/5/6.
 */
public class DataClass {
    public String row;
    public String columnFamily;
    public Map<String, Map<Long, String>> cell;

    public String getRow() {
        return row;
    }

    public void setRow(String row) {
        this.row = row;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public Map<String, Map<Long, String>> getCell() {
        return cell;
    }

    public void setCell(Map<String, Map<Long, String>> cell) {
        this.cell = cell;
    }
}
