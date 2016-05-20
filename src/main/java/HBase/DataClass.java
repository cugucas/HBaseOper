package HBase;

import java.util.Map;

/**
 * Created by LJian on 2016/5/6.
 */
public class DataClass {
    public String tableName;
    public String row;
    public String columnFamily;
    public Map<String, String> cell;//column, val
    //public Map<String, Map<Long, String>> cell;//column, timestamp, val

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

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Map<String, String> getCell() {
        return cell;
    }

    public void setCell(Map<String, String> cell) {
        this.cell = cell;
    }
}
