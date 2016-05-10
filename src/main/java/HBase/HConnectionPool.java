package HBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
//import org.apache.hadoop.hbase.client.Table;

/**
 * Created by LJian on 2016/4/7.
 */

public class HConnectionPool {

    public static Configuration configuration;

    public HConnectionPool(String zookeeperIP, String zookeeperPort){
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", zookeeperIP);
        //configuration.set("hbase.zookeeper.property.clientPort", zookeeperPort);

    }

    public HConnectionPool(Configuration conf){
        configuration = conf;
    }

    public HConnectionPool(){
        configuration = null;
    }

    public static Connection getConnection(){
        if(null == configuration) {
            configuration = HBaseConfiguration.create();
        }
        Connection conn = null;
        try {
            conn = ConnectionFactory.createConnection(configuration);
            //Table table = connection.getTable("myTable");

        }catch (IOException e){
            e.printStackTrace();

        }
        return conn;
    }

}

/*
public class HConnectionPool {
    private static Configuration conf = null;
    private static Connection conn = null ;
    static {
        try {
            conf = HBaseConfiguration. create ();
            //conf.set( "hbase.zookeeper.quorum" ,  QUORUM );
            //conf.set( "hbase.zookeeper.property.clientPort", CLIENTPORT );
            conn = ConnectionFactory.createConnection(conf);

        }  catch  (IOException e) {

            e.printStackTrace();

        }

    }

}
*/
