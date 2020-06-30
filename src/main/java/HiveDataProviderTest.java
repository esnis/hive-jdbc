

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveDataProviderTest {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";//hive驱动名称
    private static String url = "jdbc:hive2://10.6.160.196:10000/default";//连接hive2服务的连接地址，Hive0.11.0以上版本提供了一个全新的服务：HiveServer2
    private static String user = "hive2";//对HDFS有操作权限的用户
    private static String password = "";//在非安全模式下，指定一个用户运行查询，忽略密码
    private static String sql = "hive2";
    private static ResultSet res;
    public static void main(String[] args) {
        try {
            Class.forName(driverName);//加载HiveServer2驱动程序
            Connection conn = DriverManager.getConnection(url, user, password);//根据URL连接指定的数据库
            Statement stmt = conn.createStatement();

            // 执行“regular hive query”操作，此查询会转换为MapReduce程序来处理
            sql = "select count(*) from test.yongrl_rs_bingjian_with_label";
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1));
            }
            conn.close();
            conn = null;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}