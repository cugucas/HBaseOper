package HBase;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;



/**
 * Created by LJian on 2016/4/11.
 */
public class HBaseOperator {

    //public static Configuration configuration;
    public static Connection connection;
    public static HConnectionPool pool;
    public static Admin admin;

    public HBaseOperator(){
        pool = new HConnectionPool();
        connection = pool.getConnection();
    }

    public HBaseOperator(Configuration conf){
        pool = new HConnectionPool(conf);
        connection = pool.getConnection();
    }

    public HBaseOperator(String zookeeperIP, String zookeeperPort){
        pool = new HConnectionPool(zookeeperIP, zookeeperPort);
        connection = pool.getConnection();
    }

    public static void createTable(String tableName, String[] familys) throws IOException{

        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        for(String family : familys) {
            table.addFamily(new HColumnDescriptor(family));
        }
        //desc.(Compression.Algorithm.SNAPPY);

        /*
        if(null == connection){
            pool = new HConnectionPool();
            connection = pool.getConnection();
        }
        */
        connection = getConnection();
        admin = connection.getAdmin();
        //createOrOverwriteTable(admin, table);
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
        close();
    }

    /*
    public static void createOrOverwriteTable(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }
    */
    public static Connection getConnection(){
        if(null == connection){
            pool = new HConnectionPool();
            return pool.getConnection();
        }else{
            return connection;
        }
    }


    public static void close(){
        try{
            if(null != admin){
                admin.close();
            }
            if(null != connection){
                connection.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public static DataClass result2Bean(Result result){
        List<Cell> cells = result.listCells();
        DataClass rc = new DataClass();
        for(Cell cell : cells){
            rc.setRow(Bytes.toString(CellUtil.cloneRow(cell)));
            rc.setTimestamp(cell.getTimestamp());
            rc.setColumnFamily(Bytes.toString(CellUtil.cloneFamily(cell)));
            rc.setColumn(Bytes.toString(CellUtil.cloneQualifier(cell)));
        }
        return rc;
    }

    public static void addRow(String tableName, String rowKey, String colFamily, String col, String val)
        throws IOException {
        connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        table.put(put);
        table.close();
        close();
    }

    public static <T extends DataClass> void addRow(String tableName, String columnFamily, T object){
        connection = getConnection();
        Put put = new Put(object.getRow());
    }

    public static void addRows(String tableName, List<DataClass> rows) throws IOException{
        connection = getConnection();
        List<Put> putList = new ArrayList<>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        for(DataClass row : rows){
            Put p = new Put(Bytes.toBytes(row.getRow()));
            p.addColumn(Bytes.toBytes(row.getColumnFamily()), Bytes.toBytes(row.getColumn()), Bytes.toBytes(row.getValue()));
            putList.add(p);
        }
        table.put(putList);
        table.close();
        close();
    }

    public static void deleteRow(String tableName, String rowKey, String colFamily, String col)
        throws IOException{
        connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        if(null != colFamily && !colFamily.isEmpty()){
            delete.addFamily(Bytes.toBytes(colFamily));
            if(null != col && !col.isEmpty()){
                delete.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
            }
        }

        table.delete(delete);
        table.close();
        close();
    }

    public static void deleteRows(String tableName, String[] rowKeyList) throws IOException{
        connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Delete> deleteList = new ArrayList<>();
        for(String rowKey : rowKeyList){
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            deleteList.add(delete);
        }
        table.delete(deleteList);
        table.close();
        close();
    }

    public static void deleteRows(String tableName, List<DataClass> rows) throws IOException{
        connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Delete> deleteList = new ArrayList<>();
        for(DataClass row : rows){
            Delete delete = new Delete(Bytes.toBytes(row.getRow()));
            if(null != row.getColumnFamily() && !row.getColumnFamily().isEmpty()){
                delete.addFamily(Bytes.toBytes(row.getColumnFamily()));
                if(null != row.getColumn() && !row.getColumn().isEmpty()){
                    delete.addColumn(Bytes.toBytes(row.getColumnFamily()), Bytes.toBytes(row.getColumn()));
                }
            }
            deleteList.add(delete);
        }
        table.delete(deleteList);
        table.close();
        close();
    }

    public static DataClass getRow(String tableName, String rowKey, String colFamily, String col)
        throws IOException{
        connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        if(null != colFamily && !colFamily.isEmpty()){
            get.addFamily(Bytes.toBytes(colFamily));
            if(null != col && !col.isEmpty()){
                get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
            }
        }

        Result result = table.get(get);
        DataClass dc = result2Bean(result);

        table.close();
        close();
        return dc;
    }

    public static List<DataClass> getRows(String tableName, String[] rowKeyList) throws IOException{
        connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Get> getList = new ArrayList<>();
        for(String rowKey : rowKeyList){
            Get get = new Get(Bytes.toBytes(rowKey));
            getList.add(get);
        }
        Result[] results = table.get(getList);
        List<DataClass> rsList = new ArrayList<>();
        for(Result result : results){
            rsList.add(result2Bean(result));
        }

        return rsList;
    }

    public static List<DataClass> getAllRows(String tableName) throws IOException{
        connection = getConnection();
        Table table = connection.getTable(Bytes.toBytes(tableName));
        List<DataClass> rsList = new ArrayList<>();
        Scan scan = new Scan();
        ResultScanner rsc = table.getScanner(scan);
        for(Result rs : rsc){
            rsList.add(result2Bean(rs));
        }
        return rsList;
    }

    public static long getRowCount(String tableName){
        long rowCount = 0L;
        connection = getConnection();
        Table table = connection.getTable();
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setFilter(new FirstKeyOnlyFilter());
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            rowCount += 1;
        }
        Date end=new Date();
        System.out.println((end.getTime()-begin.getTime())/1000);
        return rowCount;
    }

    public static List<DataClass> scanRows(String tableName, String startRow, String stopRow) throws IOException{
        connection = getConnection();
        List<DataClass> rsList = new ArrayList<>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        if (null != startRow && !startRow.isEmpty()) {
            scan.setStartRow(Bytes.toBytes(startRow));
        }
        if (null != stopRow && !stopRow.isEmpty()) {
            scan.setStopRow(Bytes.toBytes(stopRow));
        }
        ResultScanner rsc = table.getScanner(scan);
        for(Result rs : rsc){
            rsList.add(result2Bean(rs));
        }
        return rsList;
    }

    public static List<String> listTables() throws IOException{
        connection = getConnection();
        List<String> tables = new ArrayList<>();
        admin = connection.getAdmin();
        HTableDescriptor descriptors[] = admin.listTables();
        for(HTableDescriptor des : descriptors){
            tables.add(des.getNameAsString());
        }
        close();
        return tables;
    }




}


















