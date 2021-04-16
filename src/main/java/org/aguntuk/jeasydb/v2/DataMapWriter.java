package org.aguntuk.jeasydb.v2;

import static org.aguntuk.jeasydb.v2.utils.GeneralUtils.execute;
import static org.aguntuk.jeasydb.v2.utils.GeneralUtils.generateBindVars;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.aguntuk.jeasydb.v2.utils.InsertReturn;

public class DataMapWriter extends AbstractData {
	
	public long persistMap(String tableName, 
			Map<String, Object> valueMap) throws Exception {
		StringBuffer sb = new StringBuffer("INSERT INTO ");
		return persist(tableName, valueMap, sb);
	}	
	
	private long persist(String tableName, Map<String, Object> valueMap, StringBuffer sb)
			throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, SQLException {
		StringBuffer values = new StringBuffer(" VALUES (");
		sb.append(tableName).append(" (");
		List<Object> bindVars = generateBindVars(valueMap, sb, values);
		InsertReturn ir = execute(conn, sb, values, bindVars);
		return ir.generatedKey;
	}
}
