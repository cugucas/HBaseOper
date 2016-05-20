package service.nosql;

import HBase.DataClass;
import HBase.DataObject;
import domain.HBaseData;
import HBase.HBaseOperator;
import ClassParse.ClassParse;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by LJian on 2016/5/17.
 */
public class HBaseService {

    public void createTable(String tableName, List<String> columnFamily){
        String[] list = (String[]) columnFamily.toArray();
        try {
            HBaseOperator.createTable(tableName, list);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public void deleteTable(String tableName){
        try {
            HBaseOperator.deleteTable(tableName);
        }catch (IOException e){
            e.printStackTrace();
        }
    }


    public void add(DataClass data){
        String tableName = data.getTableName();
        String rowKey = data.getRow();
        String columnFamily = data.getColumnFamily();
        Map<String, String> cell = data.getCell();
        try {
            HBaseOperator.addRow(tableName, rowKey, columnFamily, cell);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public <T extends DataObject> void add(String tableName, String columnFamily, T object){
        try {
            HBaseOperator.addRow(tableName, columnFamily, object);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public String getByRowkey(String tableName, String rowKey){
        HBaseData result = new HBaseData();
        try {
            result = HBaseOperator.getRow(tableName, rowKey, null, null);
        }catch (IOException e){
            e.printStackTrace();
        }
        return ClassParse.obj2Json(result);

    }

    public String getByRowKeyList(String tableName, List<String> rowKeyList){
        String[] list = (String[]) rowKeyList.toArray();
        HBaseData result = new HBaseData();
        try{
            result = HBaseOperator.getRows(tableName, list);
        }catch (IOException e){
            e.printStackTrace();
        }
        return ClassParse.obj2Json(result);
    }

    public String getByTime(String tableName, String startTime, String endTime){
        HBaseData result = new HBaseData();
        try{
            result = HBaseOperator.scanRowsBetweenTime(tableName, startTime, endTime);
        }catch (IOException e){
            e.printStackTrace();
        }
        return ClassParse.obj2Json(result);
    }

    public void deleteByRowKey(String tableName, String rowKey){
        try{
            HBaseOperator.deleteRow(tableName, rowKey, null, null);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public void deleteByRowKeyList(String tableName, List<String> rowKeyList){
        String[] list = (String[]) rowKeyList.toArray();
        try{
            HBaseOperator.deleteRows(tableName, list);
        }catch (IOException e){
            e.printStackTrace();
        }
    }



}
