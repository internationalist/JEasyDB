package org.aguntuk.jeasydb.v2;

import static org.aguntuk.jeasydb.v2.utils.GeneralUtils.bindPreparedStatement;
import static org.aguntuk.jeasydb.v2.utils.GeneralUtils.extractValue;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DataMapReader extends AbstractData {

	public List<Map<String, Object>> loadMap(String sql,
			Map<String, Class<? extends Object>> columnValue, Object... sqlArgs) throws SQLException {
		List<Map<String, Object>> retValue = new ArrayList<Map<String, Object>>();
		PreparedStatement stmt = conn.prepareStatement(sql);
		if (sqlArgs != null && sqlArgs.length > 0) {
			for (int i = 0; i < sqlArgs.length; i++) {
				bindPreparedStatement(i, sqlArgs[i], stmt);
			}
		}
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			Map<String, Object> row = new HashMap<String, Object>();
			for (Iterator<String> iter = columnValue.keySet().iterator(); iter
					.hasNext();) {
				String key = iter.next();
				Class<? extends Object> obj = columnValue.get(key);
				row.put(key, extractValue(key, rs, obj));
			}
			retValue.add(row);
		}
		return retValue;
	}
	
	public List<Map<String, Object>> loadMap(String sql, Object... sqlArgs) throws SQLException {
		List<Map<String, Object>> retValue = new ArrayList<Map<String, Object>>();
		PreparedStatement stmt = conn.prepareStatement(sql);
		if (sqlArgs != null && sqlArgs.length > 0) {
			for (int i = 0; i < sqlArgs.length; i++) {
				bindPreparedStatement(i, sqlArgs[i], stmt);
			}
		}
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			Map<String, Object> row = new HashMap<String, Object>();
			ResultSetMetaData rsmd = rs.getMetaData();
			for(int i = 1; i <= rsmd.getColumnCount(); ++i) {
				String key = rsmd.getColumnName(i);
				row.put(key, extractValue(key, rs));
			}
			retValue.add(row);
		}
		return retValue;
	}	
	
	
}
