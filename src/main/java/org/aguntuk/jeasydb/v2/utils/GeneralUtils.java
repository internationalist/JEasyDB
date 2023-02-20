package org.aguntuk.jeasydb.v2.utils;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.aguntuk.jeasydb.v2.AbstractData;
import org.aguntuk.jeasydb.v2.AbstractData.DBTYPE;
import org.aguntuk.jeasydb.v2.BiDiMap;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.beanutils.PropertyUtilsBean;

public enum GeneralUtils {
	I;
	PropertyUtilsBean propertyUtils;
	
	private GeneralUtils() {
		propertyUtils = new PropertyUtilsBean();
	}
	public static void bindPreparedStatement(int index, Object value, PreparedStatement stmt) throws SQLException {
		++index;
		if (value != null) {
			String name = value.getClass().getSimpleName();
			if ("int".equals(name)) {
				stmt.setInt(index, (Integer)value);
			} else if ("Integer".equals(name)) {
				stmt.setInt(index, (Integer)value);
			} else if ("float".equals(name)) {
				stmt.setFloat(index, (Float)value);				
			} else if ("Float".equals(name)) {
				stmt.setFloat(index, (Float)value);
			} else if ("long".equals(name)) {
				stmt.setLong(index, (Long)value);
			} else if ("Long".equals(name)) {
				stmt.setLong(index, (Long)value);
			} else if ("double".equals(name)) {
				stmt.setDouble(index, (Double)value);
			} else if ("Double".equals(name)) {
				stmt.setDouble(index, (Double)value);
			} else if ("String".equals(name)) {
				stmt.setString(index, (String)value);
			} else if ("Boolean".equals(name)) {
				stmt.setBoolean(index, (Boolean)value);
			} else if ("boolean".equals(name)) {
				stmt.setBoolean(index, (Boolean)value);
			}
		}
	}
	
	public static void populateBeanFromSQL(ResultSet resultset,
			Object bean, BiDiMap<String, String> propertyMapper,
			Map<String, String> propIndex) throws Exception {
		ResultSetMetaData rsmd = resultset.getMetaData();
		for (int i = 1; i <= rsmd.getColumnCount(); i++) {
			String columnName = rsmd.getColumnName(i);
			String propName = null;
			if (propertyMapper != null
					&& propertyMapper.containsValue(columnName)) {
				propName = propertyMapper.getKey(columnName);
			} else {
				propName = propIndex.get(I.normalizeColumnName(columnName, "_")).toLowerCase();
			}
			if (propName != null && propName.length() > 0) {
				Class<?> propType = I.propertyUtils.getPropertyType(bean,
						propName);
				Object value = extractValue(columnName, resultset, propType);
				if (value != null) {
					BeanUtils.setProperty(bean, propName, value);
				}
			}
		}
	}
	
	public static <T> Map<String, String> mapBeanProperty(T beanInstance) throws Exception {
		Map<String, Object> propMap = I.propertyUtils.describe(beanInstance);
		Map<String, String> propIndex = new HashMap<String, String>();
		for (Iterator<Map.Entry<String, Object>> iter = propMap.entrySet()
				.iterator(); iter.hasNext();) {
			String prop = iter.next().getKey();
			propIndex.put(prop.toLowerCase(), prop);
		}
		return propIndex;
	}
	
	private String normalizeColumnName(String s, String... spChars) {
		StringBuilder removeThis = new StringBuilder("[");
		for(String spChar:spChars) {
			removeThis.append(spChar);
		}
		removeThis.append("]");
		return s.replaceAll(removeThis.toString(), "").toLowerCase();
	}
	
	public static Object extractValue(String column,
			ResultSet resultset, Class<?> propType) throws SQLException {
			Object returnValue = null;
			if ("int".equals(propType.getSimpleName())) {
				returnValue = resultset.getInt(column);
			} else if ("Integer".equals(propType.getSimpleName())) {
				returnValue = resultset.getInt(column);
			} else if ("float".equals(propType.getSimpleName())) {
				returnValue = resultset.getFloat(column);
			} else if ("Float".equals(propType.getSimpleName())) {
				returnValue = resultset.getFloat(column);
			} else if ("long".equals(propType.getSimpleName())) {
				returnValue = resultset.getLong(column);
			} else if ("Long".equals(propType.getSimpleName())) {
				returnValue = resultset.getLong(column);
			} else if ("double".equals(propType.getSimpleName())) {
				returnValue = resultset.getDouble(column);
			} else if ("Double".equals(propType.getSimpleName())) {
				returnValue = resultset.getDouble(column);
			} else if ("Calendar".equals(propType.getSimpleName())) {
				Calendar cal = new GregorianCalendar();
				cal.setTime(resultset.getDate(column));
				returnValue = cal;
			} else if ("String".equals(propType.getSimpleName())) {
				returnValue = resultset.getString(column);
			} else if ("boolean".equals(propType.getSimpleName())) {
				returnValue = resultset.getBoolean(column);
			} else if ("Boolean".equals(propType.getSimpleName())) {
				returnValue = resultset.getBoolean(column);
			} else if ("BigDecimal".equals(propType.getSimpleName())) {
				returnValue = resultset.getBigDecimal(column);
			}
			return returnValue;
	}
	
	public static Object extractValue(String column,
			ResultSet resultset) throws SQLException {
			int idx = resultset.findColumn(column);
			ResultSetMetaData rsmd = resultset.getMetaData();
			int sqlType = rsmd.getColumnType(idx);
			
			Object returnValue = null;
			switch(sqlType) {
				case java.sql.Types.BIGINT:
					returnValue = resultset.getLong(column);
					break;
				case java.sql.Types.DOUBLE:
					returnValue = resultset.getDouble(column);
					break;
				case java.sql.Types.FLOAT:
					returnValue = resultset.getFloat(column);
					break;
				case java.sql.Types.INTEGER:
					returnValue = resultset.getInt(column);
					break;
				case java.sql.Types.BOOLEAN:
					returnValue = resultset.getBoolean(column);
					break;
				case java.sql.Types.DATE:
				case java.sql.Types.TIME:
				case java.sql.Types.TIMESTAMP:					
					Calendar cal = new GregorianCalendar();
					java.sql.Timestamp ts = resultset.getTimestamp(column);
					cal.setTime(new Date(ts.getTime()));
					returnValue = cal;
					break;
				case java.sql.Types.VARCHAR:
					returnValue = resultset.getString(column);
					break;
			}			
			return returnValue;
	}	
	
	public static <T> List<Object> generateBindVars(T bean, Map<String, String> propertyMapper,
			StringBuffer sb, StringBuffer values, AbstractData.DBTYPE dbType)
			throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		List<Object> bindVars = new ArrayList<>();		
		DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");		
		for (Iterator<String> iter = propertyMapper.keySet().iterator(); iter
				.hasNext();) {
			String propName = iter.next();
			Object value = PropertyUtils.getSimpleProperty(bean, propName);
			if (value != null) {
				sb.append(propertyMapper.get(propName)).append(",");
				Class<?> propType = I.propertyUtils.getPropertyType(bean,
						propName);
				if (propType.getSimpleName().indexOf("Calendar") > -1) {
					Calendar timeValue = (Calendar) value;
					String timeValueStr = df.format(timeValue.getTime());
					switch(dbType) {
						case mysql:
							values.append("STR_TO_DATE(?, '%Y%m%d%h%i%s'),");
							break;
						case oracle:
							values.append("TO_DATE(?, 'YYYYMMDDHH24MISS'),");							
							break;
						case sqlite:
							break;
					}
					bindVars.add(timeValueStr);
				} else {
					values.append("?,");
					bindVars.add(value);
				}
			}
		}
		return bindVars;
	}
	
	public static List<Object> generateBindVars(Map<String, Object> valueMap,
			StringBuffer sb, StringBuffer values, AbstractData.DBTYPE dbType)
			throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		List<Object> bindVars = new ArrayList<>();		
		DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");		
		for (Iterator<String> iter = valueMap.keySet().iterator(); iter
				.hasNext();) {
			String propName = iter.next();
			Object value = valueMap.get(propName);
			if (value != null) {
				sb.append(propName).append(",");
				Class<?> propType = value.getClass();
				if (propType.getSimpleName().indexOf("Calendar") > -1) {				
					Calendar timeValue = (Calendar) value;
					String timeValueStr = df.format(timeValue.getTime());
					switch(dbType) {
						case mysql:
							values.append("STR_TO_DATE(?, '%Y%m%d%H%i%s'),");
							break;
						case oracle:
							values.append("TO_DATE(?, 'YYYYMMDDHH24MISS'),");							
							break;
						case sqlite:
							break;
					}
					bindVars.add(timeValueStr);
				} else {
					values.append("?,");
					bindVars.add(value);
				}
			}
		}
		return bindVars;
	}	
	
	public static <T> Map<String, String> derivePropMap(T bean)
			throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		List<String> propNames = I.getPropertyList(I.propertyUtils.describe(bean));
		Map<String, String> propMap = new HashMap<>();
		for(String propName: propNames) {
			char[] propNameChars = propName.toCharArray();
			List<Character> charList = new ArrayList<>(); 
			for(int i = 0; i < propNameChars.length; i++) {
				if(i == 0) {
					charList.add(Character.toLowerCase(propNameChars[i]));
				} else if(Character.isUpperCase(propNameChars[i])) {
					charList.add('_');
					charList.add(Character.toLowerCase(propNameChars[i]));
				} else {
					charList.add(Character.toLowerCase(propNameChars[i]));
				}
			}
	        propMap.put(propName, I.charListToString(charList)); 
		}
		return propMap;
	}
	
	public static InsertReturn execute(Connection conn, StringBuffer sb, StringBuffer values, List<Object> bindVars) throws SQLException {
		InsertReturn ir = new InsertReturn();
		String sqlA = sb.substring(0, sb.lastIndexOf(",")) + ")";
		String sqlB = values.substring(0, values.lastIndexOf(",")) + ")";
		StringBuffer params = new StringBuffer();
		System.out.println("SQL is " + sqlA + sqlB);
		PreparedStatement ps = conn.prepareStatement(sqlA + sqlB, Statement.RETURN_GENERATED_KEYS);
		for(int i = 0; i < bindVars.size(); i++) {
			bindPreparedStatement(i, bindVars.get(i), ps);
			params.append(bindVars.get(i)).append(",");
		}
		System.out.println(params.substring(0, params.lastIndexOf(",")));
		ir.rows = ps.executeUpdate();
		ResultSet generatedKeys = ps.getGeneratedKeys();
		while(generatedKeys.next()) {
			ir.generatedKey=generatedKeys.getLong(1);
		}
		return ir;
	}	
	
	private List<String> getPropertyList(Map<String, Object> propMap) {
		List<String> propList = new ArrayList<>();
		Set<String> propSet = propMap.keySet();
		for(String prop:propSet) {
			if(!"class".equalsIgnoreCase(prop)) {
				propList.add(prop);
			}
		}
		return propList;
	}
	
	private String charListToString(List<Character> charList) {
		StringBuilder sb = new StringBuilder(); 
		
		// Appends characters one by one 
		for (Character ch : charList) { 
		    sb.append(ch); 
		}
		return sb.toString();
	}	
}
