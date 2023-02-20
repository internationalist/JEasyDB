package org.aguntuk.jeasydb.v2;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.dbcp.BasicDataSource;

//TODO: Not thread safe
public enum DataSourceFactory {
i;
	Connection conn;
	Properties props;
	private DataSourceFactory() {
		props = new Properties();
		try {
			props.load(ClassLoader.getSystemResourceAsStream("db.ini"));
			conn = createConnection(props);
			conn.setAutoCommit(true);
		} catch (IOException | SQLException e) {
			e.printStackTrace();
		}	
	}
	private void setup() {
	
	}
	
	public static Connection getConnection() {
		return i.conn;
	}
	
	public static void tearDown() {
		try {
			i.conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private Connection createConnection(Properties props) throws SQLException {
		BasicDataSource ds = new BasicDataSource();
		ds.setDriverClassName(props.getProperty("DB_DRIVER_CLASS"));
		ds.setUrl(props.getProperty("DB_URL"));
		if(props.getProperty("DB_USERNAME") != null) {
			ds.setUsername(props.getProperty("DB_USERNAME"));
		}
		if(props.getProperty("DB_PASSWORD") != null) {			
			ds.setPassword(props.getProperty("DB_PASSWORD"));
		}
		return ds.getConnection();
	}	
}
