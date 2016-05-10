package HBase;

import org.apache.hadoop.hbase.client.Connection;
import org.junit.Test;



import static org.junit.Assert.*;

/**
 * Created by LJian on 2016/4/19.
 */
public class HBaseOperatorTest {

    @Test
    public void testCreateTable() throws Exception {
        String tableName = "TestTable";
        String[] familys = new String[]{"fam1", "fam2"};
        HBaseOperator.createTable(tableName, familys);
    }

    @Test
    public void testCreateOrOverwriteTable() throws Exception {

    }

    @Test
    public void testGetConnection() throws Exception {
        Connection con = HBaseOperator.getConnection();
    }

    @Test
    public void testClose() throws Exception {

    }

    @Test
    public void testAddRow() throws Exception {
        String tableName = "TestTable";
        String rowKey = "row1";
        String colFamily = "fam1";
        String col = "col1";
        String val = "val1";

        HBaseOperator.addRow(tableName, rowKey, colFamily, col, val);
    }

    @Test
    public void testAddRows() throws Exception {

    }

    @Test
    public void testDeleteRow() throws Exception {

    }

    @Test
    public void testDeleteRows() throws Exception {

    }

    @Test
    public void testGetRow() throws Exception {

    }
}
