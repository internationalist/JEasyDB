package org.aguntuk.jeasydb.v2;

import static org.aguntuk.jeasydb.v2.utils.GeneralUtils.bindPreparedStatement;
import static org.aguntuk.jeasydb.v2.utils.GeneralUtils.execute;
import static org.aguntuk.jeasydb.v2.utils.GeneralUtils.generateBindVars;

import java.lang.reflect.InvocationTargetException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.aguntuk.jeasydb.v2.utils.GeneralUtils;
import org.aguntuk.jeasydb.v2.utils.InsertReturn;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.beanutils.PropertyUtilsBean;

public class DataObjectWriter extends AbstractData {
	
	
	public DataObjectWriter() {
		super();
	}	
	
	public <T> long persist(String tableName, T bean)
			throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, SQLException {
		//get property map from list of property names.
		Map<String, String> propMap = GeneralUtils.derivePropMap(bean);
		return persist(tableName, bean, propMap);
	}
	
	public <T> long persist(String tableName, T bean, Map<String, String> propertyMapper)
			throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, SQLException {
		StringBuffer values = new StringBuffer(" VALUES (");
		StringBuffer sb = new StringBuffer(tableName).append(" (");
		List<Object> bindVars = generateBindVars(bean, propertyMapper, sb, values);
		InsertReturn ir = execute(conn, sb, values, bindVars);
		return ir.generatedKey;
	}
	

	
	public <T> int updateObject(String tableName, T bean, String... whereCols)
			throws Throwable {
		Map<String, String> propMap = GeneralUtils.derivePropMap(bean);
		return updateObject(tableName, bean, propMap, whereCols);
	}
	
	public <T> int updateObject(String tableName, T bean,
			Map<String, String> propertyMapper, String... whereCols)
			throws Throwable {
		PropertyUtilsBean propertyUtils = new PropertyUtilsBean();
		DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		List<Object> setBindVars = new ArrayList<>();
		List<Object> clauseBindVars = new ArrayList<>();
		StringBuffer sb = new StringBuffer("UPDATE ").append(tableName).append(
				" SET ");
		StringBuffer where = new StringBuffer(" WHERE ");
		List<String> whereColList = null;
		if (whereCols != null) {
			whereColList = Arrays.asList(whereCols);
		}

		for (Iterator<String> iter = propertyMapper.keySet().iterator(); iter
				.hasNext();) {
			String propName = iter.next();
			Object value = PropertyUtils.getSimpleProperty(bean, propName);
			if (value != null) {
				if (whereColList != null && whereColList.contains(propName)) {
					where.append(propertyMapper.get(propName)).append("=");
					Class<?> propType = propertyUtils.getPropertyType(bean,
							propName);
					if ("Calendar".equalsIgnoreCase(propType.getSimpleName())) {
						Calendar timeValue = (Calendar) value;
						String timeValueStr = df.format(timeValue.getTime());
						where.append("TO_DATE(?, 'YYYYMMDDHH24MISS') and ");
						clauseBindVars.add(timeValueStr);
					} else {
						where.append("? and ");
						clauseBindVars.add(value);
					}
				} else {
					sb.append(propertyMapper.get(propName)).append("=?,");
					setBindVars.add(value);
				}

			}
		}
		String sqlA = null;
		String sqlB = null;

		if (sb.lastIndexOf(",") > -1) {
			sqlA = sb.substring(0, sb.lastIndexOf(","));
		} else {
			sqlA = sb.toString();
		}

		if (where.lastIndexOf("and") > -1) {
			sqlB = where.substring(0, where.lastIndexOf("and"));
		} else {
			sqlB = where.toString();
		}
		String finalSQL = sqlA;
		if (where.length() > 7) {
			finalSQL = sqlA + sqlB;
		}
		PreparedStatement ps = conn.prepareStatement(finalSQL);
		System.out.println("update SQL is " + finalSQL);
		setBindVars.addAll(clauseBindVars);
		for(int i = 0; i < setBindVars.size(); i++) {
			bindPreparedStatement(i, setBindVars.get(i), ps);			
		}
		
		System.out.println("Bind variables are " + setBindVars);
		int rows = 0;
		rows = ps.executeUpdate();
		return rows;
	}	
}
