package org.aguntuk.jeasydb.v2;

import static org.aguntuk.jeasydb.v2.utils.GeneralUtils.bindPreparedStatement;

import java.sql.Connection;
import java.sql.PreparedStatement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.aguntuk.jeasydb.v2.utils.DynamicSQLUtil;
import org.aguntuk.jeasydb.v2.utils.InsertReturn;

public class AbstractData {

	protected Connection conn;

	public AbstractData() {
		conn = DataSourceFactory.getConnection();
	}
	
	public String generateDynamicSQL(String sql, Map<String, Object> dynamicSQLargs, Map<String, Integer> bindCountMap) {
		if(dynamicSQLargs != null && dynamicSQLargs.size() > 0
				&& bindCountMap != null && bindCountMap.size() > 0) {
			sql = DynamicSQLUtil.instance.formSQL(sql, dynamicSQLargs, bindCountMap);
		} else if(bindCountMap != null && bindCountMap.size() > 0) {
			sql = DynamicSQLUtil.instance.generateInBinds(sql, bindCountMap);
		} else if(dynamicSQLargs != null && dynamicSQLargs.size() > 0) {
			sql = DynamicSQLUtil.instance.formSQL(sql, dynamicSQLargs);			
		}
		return sql;
	}
	
	public InsertReturn runUpdate(String sql, Object... sqlArgs) throws SQLException  {
		InsertReturn ir = new InsertReturn();
		PreparedStatement ps = conn.prepareStatement(sql);
		if (sqlArgs != null) {
			for (int i = 0; i < sqlArgs.length; i++) {
				bindPreparedStatement(i, sqlArgs[i], ps);
			}
		}
		ir.rows = ps.executeUpdate();
		ResultSet generatedKeys = ps.getGeneratedKeys();
		while(generatedKeys.next()) {
			ir.generatedKey=generatedKeys.getInt(1);
		}		
		return ir;
	}	
	
	
	
}
