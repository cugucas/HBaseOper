package domain;

import java.util.List;

/**
 * Created by LJian on 2016/5/19.
 */
public class HBaseData {
    public String tableName;
    public List<HBaseRowData> rows;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<HBaseRowData> getRows() {
        return rows;
    }

    public void setRows(List<HBaseRowData> rows) {
        this.rows = rows;
    }
}
