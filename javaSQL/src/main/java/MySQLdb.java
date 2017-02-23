import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.*;

/**
 * Created by smitty on 2/23/2017.
 */
public class MySQLdb {

    private Connection connection;

    private final String USERNAME;
    private final String PASSWORD;
    private final String HOST;

    public MySQLdb(String host, String username, String password) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        HOST = host;
        USERNAME = username;
        PASSWORD = password;
    }

    private void connect() throws SQLException {
        connection = DriverManager.getConnection(HOST, USERNAME, PASSWORD);
        if (!connection.isClosed()) System.out.println("Connected Successfully!");
        else
            throw new RuntimeException(String.format("Failed to connect to the database(%s) with User: %s and Pass: %s", HOST, USERNAME, PASSWORD));
    }

    private void disconnect() throws SQLException {
        if (!connection.isClosed()) {
            connection.close();
            if (connection.isClosed()) System.out.println("Connection Closed!");
            else
                throw new RuntimeException(String.format("Failed to disconnect from the database(%s) with User: %s and Pass: %s", HOST, USERNAME, PASSWORD));
        }
    }

    public JSONArray runQuery(String query) throws SQLException {
        System.out.println("Query=" + query);
        try {
            connect();
            PreparedStatement statement = connection.prepareStatement(query);
            return MySQLdb.convert(statement.executeQuery());
        } finally {
            disconnect();
        }
    }

    public void runUpdate(String query) throws SQLException {
        System.out.println("Query=" + query);
        try {
            connect();
            PreparedStatement statement = connection.prepareStatement(query);
            statement.executeUpdate();
        } finally {
            disconnect();
        }
    }

    public static JSONArray convert(ResultSet results) throws SQLException, JSONException {
        JSONArray jsonArray = new JSONArray();
        if (results != null) {
            System.out.println("Converting ResultSet to JSONArray...");
            ResultSetMetaData metadata = results.getMetaData();
            int numColumns = metadata.getColumnCount();

            while (results.next()) {
                JSONObject jsonObject = new JSONObject();
                for (int i = 1; i < numColumns + 1; i++) {

                    String column_name = metadata.getColumnName(i);
                    int column_type = metadata.getColumnType(i);

                    if (column_type == Types.ARRAY) jsonObject.put(column_name, results.getArray(column_name));
                    else if (column_type == Types.BIGINT) jsonObject.put(column_name, results.getInt(column_name));
                    else if (column_type == Types.BOOLEAN) jsonObject.put(column_name, results.getBoolean(column_name));
                    else if (column_type == Types.BLOB) jsonObject.put(column_name, results.getBlob(column_name));
                    else if (column_type == Types.DOUBLE) jsonObject.put(column_name, results.getDouble(column_name));
                    else if (column_type == Types.FLOAT) jsonObject.put(column_name, results.getFloat(column_name));
                    else if (column_type == Types.INTEGER) jsonObject.put(column_name, results.getInt(column_name));
                    else if (column_type == Types.NVARCHAR)
                        jsonObject.put(column_name, results.getNString(column_name));
                    else if (column_type == Types.VARCHAR) jsonObject.put(column_name, results.getString(column_name));
                    else if (column_type == Types.VARBINARY)
                        jsonObject.put(column_name, results.getString(column_name));
                    else if (column_type == Types.TINYINT) jsonObject.put(column_name, results.getInt(column_name));
                    else if (column_type == Types.SMALLINT) jsonObject.put(column_name, results.getInt(column_name));
                    else if (column_type == Types.DATE) jsonObject.put(column_name, results.getDate(column_name));
                    else if (column_type == Types.TIMESTAMP)
                        jsonObject.put(column_name, results.getTimestamp(column_name));
                    else jsonObject.put(column_name, results.getObject(column_name));
                }
                jsonArray.put(jsonObject);
            }
        } else System.out.println("No Results");
        return jsonArray;
    }
}