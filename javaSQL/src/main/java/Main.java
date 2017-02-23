import org.json.JSONArray;

/**
 * Created by smitty on 2/23/2017.
 */
public class Main {

    public static void main(String[] args) {
        MySQLdb mySQLdb = new MySQLdb();
        JSONArray results = mySQLdb.runQuery("select * from results");
        for(Object result : results) System.out.println(result);
    }
}
