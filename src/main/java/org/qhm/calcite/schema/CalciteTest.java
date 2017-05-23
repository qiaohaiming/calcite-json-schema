package org.qhm.calcite.schema;

import java.sql.ResultSet;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import com.alibaba.fastjson.JSONObject;

import java.sql.*;

public class CalciteTest {
	public static void main(String[] args) throws Exception {
		long begin = System.currentTimeMillis();
		new CalciteTest().run();
		long duration = System.currentTimeMillis() - begin;
		System.out.println("total:" + duration);
	}

	public void run() throws ClassNotFoundException, SQLException {
		Class.forName("org.apache.calcite.jdbc.Driver");
		Connection connection = DriverManager.getConnection("jdbc:calcite:");
		CalciteConnection optiqConnection = (CalciteConnection) connection.unwrap(CalciteConnection.class);
		SchemaPlus rootSchema = optiqConnection.getRootSchema();

		String json = "[{\"CUST_ID\":{\"a\":1},\"PROD_ID\":220,\"USER_ID\":300,\"USER_NAME\":\"user1\"},"
				+ "{\"USER_ID\":310,\"CUST_ID\":{\"a\":2},\"PROD_ID\":210,\"USER_NAME\":\"user2\"}]";

		Statement statement = connection.createStatement();
		

		ResultSet resultSet = null;
		long begin = System.currentTimeMillis();
		for (int i = 0; i < 1; i++) {
			rootSchema.add("json", new JsonSchema("a/b/c", json));
			resultSet = statement.executeQuery(
					"select PROD_ID*10 as a, USER_ID, CUST_ID from \"json" + "\".\"a/b/c\" where USER_ID>100");
		}
		System.out.println("query:" + (System.currentTimeMillis() - begin));

		while (resultSet.next()) {
			JSONObject jo = new JSONObject();
			int n = resultSet.getMetaData().getColumnCount();
			for (int i = 1; i <= n; i++) {
				jo.put(resultSet.getMetaData().getColumnName(i), resultSet.getObject(i));
			}
			System.out.println(jo.toJSONString());
		}
		resultSet.close();
		statement.close();
		connection.close();
	}
}

// End JdbcExample.java
