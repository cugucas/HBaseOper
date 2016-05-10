package HBase;

/**
 * Created by LJian on 2016/5/6.
 */
public class DataClass {
    public String row;
    public String columnFamily;
    public String column;
    public String value;
    public Long timestamp;

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

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
