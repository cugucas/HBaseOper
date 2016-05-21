package HBase;

import domain.HBaseRowData;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.Test;



import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        String rowKey = "2010010112204400020003";
        String colFamily = "fam1";
        String col = "col1";
        String val = "val1";

        HBaseOperator.addRow(tableName, rowKey, colFamily, col, val);
    }


    @Test
    public void testDeleteRow() throws Exception {
        String tableName = "TestTable";
        String rowKey = "2011010112204400020003";
        HBaseOperator.deleteRow(tableName, rowKey, null, null);
    }

    @Test
    public void testDeleteRows() throws Exception {
        String tableName = "TestTable";
        List<String> rows = new ArrayList<>();
        rows.add("2012010112204400020003");
        rows.add("2013010112204400020003");
        HBaseOperator.deleteRows(tableName, rows);
    }

    @Test
    public void testGetRow() throws Exception {
        String tableName = "TestTable";
        String rowKey = "2011010112204400020003";
        HBaseOperator.getRow(tableName, rowKey);
    }

    @Test
    public void testResults2Bean() throws Exception {

    }

    @Test
    public void testResults2Bean1() throws Exception {

    }


    @Test
    public void testDeleteTable() throws Exception {

    }

    @Test
    public void testAddRow1() throws Exception {
        String tableName = "TestTable";
        String rowKey = "2014010112204400020003";
        String colFamily = "fam1";
        Map<String, String> cell = new HashMap<>();
        cell.put("col1", "val1");
        cell.put("col2", "val2");
        cell.put("col3", "val3");
        HBaseOperator.addRow(tableName, rowKey, colFamily, cell);
    }

    @Test
    public void testAddRow2() throws Exception {
        String tableName = "TestTable";
        String colFamily = "fam1";
        HBaseRowData testRow = new HBaseRowData();
        testRow.setRowKey("2015010112204400020003");
        Map<String, String> cell = new HashMap<>();
        cell.put("col1", "Tval1");
        cell.put("col2", "Tval2");
        cell.put("col3", "Tval3");
        testRow.setCells(cell);
        HBaseOperator.addRow(tableName, colFamily, testRow);
    }


    @Test
    public void testGetRows() throws Exception {
        String tableName = "TestTable";
        List<String> rows = new ArrayList<>();
        rows.add("2012010112204400020003");
        rows.add("2013010112204400020003");
        HBaseOperator.getRows(tableName, rows);
    }

    @Test
    public void testGetAllRows() throws Exception {
        String tableName = "TestTable";
        HBaseOperator.getAllRows(tableName);
    }

    @Test
    public void testGetRowCount() throws Exception {
        String tableName = "TestTable";
        Long count = HBaseOperator.getRowCount(tableName);
    }

    @Test
    public void testScanRows() throws Exception {
        String tableName = "TestTable";
        String startRow = "2011010112204400020003";
        String stopRow = "2015110112204400020003";
        HBaseOperator.scanRows(tableName, startRow, stopRow);
    }

    @Test
    public void testScanRowsBetweenTime() throws Exception {
        String tableName = "TestTable";
        String startTime = "20110101122044";
        String endTime = "20151101122044";
        HBaseOperator.scanRowsBetweenTime( tableName, startTime,  endTime);
    }

    @Test
    public void testListTables() throws Exception {
        List<String> tableList = new ArrayList<>();
        tableList = HBaseOperator.listTables();
    }
}
