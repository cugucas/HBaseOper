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
    public void testGetConnection() throws Exception {
        Connection conn = HBaseOperator.getConnection();
        if(null != conn){
            System.out.println("Connection got");
        }
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

    @Test
    public void testResults2Bean() throws Exception {

    }

    @Test
    public void testResults2Bean1() throws Exception {

    }

    @Test
    public void testCreateTable1() throws Exception {

    }

    @Test
    public void testDeleteTable() throws Exception {

    }

    @Test
    public void testAddRow1() throws Exception {

    }

    @Test
    public void testAddRow2() throws Exception {

    }

    @Test
    public void testAddRow3() throws Exception {

    }

    @Test
    public void testDeleteRow1() throws Exception {

    }

    @Test
    public void testDeleteRows1() throws Exception {

    }

    @Test
    public void testGetRow1() throws Exception {

    }

    @Test
    public void testGetRows() throws Exception {

    }

    @Test
    public void testGetAllRows() throws Exception {

    }

    @Test
    public void testGetRowCount() throws Exception {

    }

    @Test
    public void testScanRows() throws Exception {

    }

    @Test
    public void testScanRowsBetweenTime() throws Exception {

    }

    @Test
    public void testListTables() throws Exception {

    }
}
