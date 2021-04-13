package org.aguntuk.jeasydb;

import java.lang.reflect.InvocationTargetException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.aguntuk.jeasydb.utils.DBSpecificSQLFrags;
import org.aguntuk.jeasydb.utils.DBTypes;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.beanutils.PropertyUtilsBean;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;

public class JEasyDB {
	private static final Logger logger = Logger.getLogger(JEasyDB.class);
	private PropertyUtilsBean propertyUtils;
	private Connection conn;

	public static enum PARAMTYPE {
		IN, OUT, INOUT;
	}

	public JEasyDB(Properties props) {
        try {		
			propertyUtils = new PropertyUtilsBean();
			BasicDataSource ds = new BasicDataSource();
			ds.setDriverClassName(props.getProperty("DB_DRIVER_CLASS"));
			ds.setUrl(props.getProperty("DB_URL"));
			if(props.getProperty("DB_USERNAME") != null) {
				ds.setUsername(props.getProperty("DB_USERNAME"));
			}
			if(props.getProperty("DB_PASSWORD") != null) {			
				ds.setPassword(props.getProperty("DB_PASSWORD"));
			}
			conn = ds.getConnection();
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			logger.error(e);
			e.printStackTrace();
		}
	}

	public void startTransaction() throws SQLException {
		conn.setAutoCommit(false);
	}

	public void stopTransaction() throws SQLException {
		conn.setAutoCommit(true);
	}

	public void commitTransaction() throws SQLException {
		conn.commit();
	}

	public void rollbackTransaction() throws SQLException {
		conn.rollback();
	}

	public <T> List<T> loadObject(String sql, Class<T> bean,
			Map<String, String> propertyMapper, Map<String, Object> dynamicSQLargs)
			throws Exception {
		return loadObject(sql, bean, propertyMapper, dynamicSQLargs, new Object[0]);
	}	
	/**
	 * @param sql
	 * @param bean
	 * @param propertyMapper
	 * @param dynamicSQLargs
	 * @param sqlArgs
	 * @return
	 * @throws Exception
	 * Load Object with support for dynamic SQL. Dynamic SQLs support If constructs for the time being. More might be added later.
	 * Example: select * from people_table where first_name = ? {{if address != null}}address like ?{{/if} and last_name = ?
	 * To use the above dynamic SQL create a new Map object with key of 'address' and value as whatever value you would like to provide.
	 * Example:
	 * 		Map<String, Object> dynamicSQLArgs = new HashMap<String, Object>();
	 *		dynamicSQLArgs.put("address", "800 Connecticut%");
	 * Then invoke this method and pass in the dynamicSQL map object.
	 * The sql that will be invoked will be: select * from people_table where first_name = ? and address like ? and last_name = ?
	 * At runtime the passed in value in the Map object corresponding to the same key is used.
	 * if you want to skip the dynamic part, do not populate the key in the map or pass null instead of the map.
	 */
	public <T> List<T> loadObject(String sql, Class<T> bean,
			Map<String, String> propertyMapper, Map<String, Object> dynamicSQLargs, Object... sqlArgs)
			throws Exception {
		sql = DynamicSQLUtil.instance.formSQL(sql, dynamicSQLargs);
		return loadObject(sql, bean, propertyMapper, sqlArgs);
	}
	
	public <T> List<T> loadObject(String sql, Class<T> bean,
			Map<String, String> propertyMapper, Map<String, Object> dynamicSQLargs, Map<String, Integer> bindCountMap, Object... sqlArgs)
			throws Exception {
		sql = DynamicSQLUtil.instance.formSQL(sql, dynamicSQLargs, bindCountMap);
		return loadObject(sql, bean, propertyMapper, sqlArgs);
	}	
	
	public <T> List<T> loadObject(String sql, Class<T> bean,
			Map<String, String> propertyMapper) throws Exception {
		return loadObject(sql, bean, propertyMapper, new Object[0]);
	}	

	public <T> List<T> loadObject(String sql, Class<T> bean,
			Map<String, String> propertyMapper, Object... sqlArgs)
			throws Exception {
		List<T> retValue = new ArrayList<T>();
		PreparedStatement stmt = conn.prepareStatement(sql);
		if (sqlArgs != null && sqlArgs.length > 0) {
			for (int i = 0; i < sqlArgs.length; i++) {
				bindPreparedStatement(i, sqlArgs[i], stmt);
			}
		}
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
				T bInstance = bean.newInstance();
				populateBeanFromSQL(rs, bInstance, propertyMapper);
				retValue.add(bInstance);
		}
		return retValue;
	}
	
	/**
	 * @param sql
	 * @param dynamicSQLargs
	 * @param sqlArgs
	 * @return
	 * @throws Exception
	 * getDataAsVector with support for dynamic SQL. Dynamic SQLs support If constructs for the time being. More might be added later.
	 * Example: select * from people_table where first_name = ? {if address != null}address like ?{/if} and last_name = ?
	 * To use the above dynamic SQL create a new Map object with key of 'address' and value as whatever value you would like to provide.
	 * Example:
	 * 		Map<String, Object> dynamicSQLArgs = new HashMap<String, Object>();
	 *		dynamicSQLArgs.put("address", "800 Connecticut%");
	 * Then invoke this method and pass in the dynamicSQL map object.
	 * The sql that will be invoked will be: select * from people_table where first_name = ? and address like ? and last_name = ?
	 * At runtime the passed in value in the Map object corresponding to the same key is used.
	 * if you want to skip the dynamic part, do not populate the key in the map or pass null instead of the map.
	 */
	public List<Map<String, Object>> getDataAsList(String sql, Map<String, Object> dynamicSQLargs, Object... sqlArgs) throws Exception {
		return getDataAsList(sql, dynamicSQLargs, null, sqlArgs);
	}	
	public List<Map<String, Object>> getDataAsList(String sql, Map<String, Object> dynamicSQLargs,
			Map<String, Integer> bindCountMap,  Object... sqlArgs)
			throws Exception {
		sql = DynamicSQLUtil.instance.formSQL(sql, dynamicSQLargs, bindCountMap);
		return getDataAsList(sql, sqlArgs);
	}
	
	public List<Map<String, Object>> getDataAsList(String sql) throws Exception {
		return getDataAsList(sql, new Object[0]);
	}
	
	public List<Map<String, Object>> getDataAsList(String sql,
			Object... sqlArgs) throws Exception {
		List<Map<String, Object>> op = new ArrayList<Map<String, Object>>();
		PreparedStatement stmt = conn.prepareStatement(sql);		
		if (sqlArgs != null && sqlArgs.length > 0) {
			for (int i = 0; i < sqlArgs.length; i++) {
				bindPreparedStatement(i, sqlArgs[i], stmt);				
			}
		}
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
				op.add(convertRSToMap(rs));
		}		
		return op;
	}

	public <T> int updateObject(String tableName, T bean,
			Map<String, String> propertyMapper, String... whereCols)
			throws Throwable {
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
	
	public int persistMap(String tableName, 
			Map<String, Object> valueMap) throws Exception {
		StringBuffer sb = new StringBuffer("INSERT INTO ");
		return persist(tableName, valueMap, sb);
	}

	private int persist(String tableName, Map<String, Object> valueMap, StringBuffer sb)
			throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, SQLException {
		StringBuffer values = new StringBuffer(" VALUES (");
		sb.append(tableName).append(" (");
		List<Object> bindVars = generateBindVars(valueMap, sb, values);
		InsertReturn ir = execute(sb, values, bindVars);
		return ir.generatedKey;
	}
	
	public int mergeMap(String tableName, 
			Map<String, Object> valueMap, DBTypes dbType) throws Exception {
		StringBuffer sb = new StringBuffer(DBSpecificSQLFrags.instance.getMergeFrag(dbType));
		return persist(tableName, valueMap, sb);
	}	

	public <T> int persistObject(String tableName, T bean,
			Map<String, String> propertyMapper) throws Exception {
		StringBuffer sb = new StringBuffer("INSERT INTO ");
		return persist(tableName, bean, propertyMapper, sb);
	}
	
	public <T> int persistIgnoreObject(String tableName, T bean,
			Map<String, String> propertyMapper) throws Exception {
		StringBuffer sb = new StringBuffer("INSERT or ignore INTO ");
		return persist(tableName, bean, propertyMapper, sb);
	}	
	
	public <T> int mergeObject(String tableName, T bean,
			Map<String, String> propertyMapper, DBTypes dbType) throws Exception {
		StringBuffer sb = new StringBuffer(DBSpecificSQLFrags.instance.getMergeFrag(dbType));
		return persist(tableName, bean, propertyMapper, sb);
	}

	private <T> int persist(String tableName, T bean, Map<String, String> propertyMapper, StringBuffer sb)
			throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, SQLException {
		StringBuffer values = new StringBuffer(" VALUES (");
		sb.append(tableName).append(" (");
		List<Object> bindVars = generateBindVars(bean, propertyMapper, sb, values);
		InsertReturn ir = execute(sb, values, bindVars);
		return ir.generatedKey;
	}
	
	/**
	 * @param sql
	 * @param columnValue
	 * @param dynamicSQLargs
	 * @param sqlArgs
	 * @return
	 * @throws Exception
	 * loadArray with support for dynamic SQL. Dynamic SQLs support If constructs for the time being. More might be added later.
	 * Example: select * from people_table where first_name = ? {{if address != null}}address like ?{{/if} and last_name = ?
	 * To use the above dynamic SQL create a new Map object with key of 'address' and value as whatever value you would like to provide.
	 * Example:
	 * 		Map<String, Object> dynamicSQLArgs = new HashMap<String, Object>();
	 *		dynamicSQLArgs.put("address", "800 Connecticut%");
	 * Then invoke this method and pass in the dynamicSQL map object.
	 * The sql that will be invoked will be: select * from people_table where first_name = ? and address like ? and last_name = ?
	 * At runtime the passed in value in the Map object corresponding to the same key is used.
	 * if you want to skip the dynamic part, do not populate the key in the map or pass null instead of the map.
	 */
	public List<Map<String, Object>> loadMap(String sql, Map<String,
			Class<? extends Object>> columnValue, Map<String, Object> dynamicSQLargs, Object... sqlArgs)
			throws Exception {
		sql = DynamicSQLUtil.instance.formSQL(sql, dynamicSQLargs);
		return loadMap(sql, columnValue, sqlArgs);
	}

	public List<Map<String, Object>> loadMap(String sql, Map<String,
			Class<? extends Object>> columnTypes,
			Map<String, Object> dynamicSQLargs, Map<String, Integer> bindCountMap, Object... sqlArgs)
					throws SQLException {
		sql = DynamicSQLUtil.instance.formSQL(sql, dynamicSQLargs, bindCountMap);
		return loadMap(sql, columnTypes, sqlArgs);		
	}
	

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

	public InsertReturn runUpdate(String sql) throws SQLException {
		return runUpdate(sql, (Object[]) null);
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
	
	private Map<String, Object> convertRSToMap(ResultSet rs) throws SQLException {
		Map<String, Object> rowData = new HashMap<String, Object>();
		ResultSetMetaData metadata = rs.getMetaData();
	    int columnCount = metadata.getColumnCount();    
	    for (int i = 1; i <= columnCount; i++) {
	    	rowData.put(metadata.getColumnLabel(i), rs.getObject(i));
	    }
	    return rowData;
	}	
	
	private void populateBeanFromSQL(ResultSet resultset,
			Object bean, Map<String, String> propertyMapper) throws Exception {

		for (Iterator<String> iter = propertyMapper.keySet().iterator(); iter
				.hasNext();) {
			String propName = iter.next();
			Class<?> propType = propertyUtils.getPropertyType(bean, propName);
			Object value = extractValue(propertyMapper.get(propName),
					resultset, propType);
			if (value != null) {
				BeanUtils.setProperty(bean, propName, value);
			}
		}
	}

	private Object extractValue(String column,
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

	private void bindPreparedStatement(int index, Object value, PreparedStatement stmt) throws SQLException {
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

	private Map<Integer, Integer> registerStoredProcParams(
			Map<Object, PARAMTYPE> paramdefs, CallableStatement cs) throws SQLException {
		int index = 0;
		Map<Integer, Integer> outParams = new HashMap<Integer, Integer>();
		for (Iterator<Object> iter = paramdefs.keySet().iterator(); iter
				.hasNext();) {
			Object value = iter.next();
			PARAMTYPE inoutparam = paramdefs.get(value);
			outParams.putAll(StoredProcParamPlumber.instance.registerParams(value, inoutparam, cs, index));
			++index;
		}
		return outParams;
}	

	private InsertReturn execute(StringBuffer sb, StringBuffer values, List<Object> bindVars) throws SQLException {
		InsertReturn ir = new InsertReturn();
		String sqlA = sb.substring(0, sb.lastIndexOf(",")) + ")";
		String sqlB = values.substring(0, values.lastIndexOf(",")) + ")";
		StringBuffer params = new StringBuffer();
		System.out.println("SQL is " + sqlA + sqlB);
		PreparedStatement ps = conn.prepareStatement(sqlA + sqlB);
		for(int i = 0; i < bindVars.size(); i++) {
			bindPreparedStatement(i, bindVars.get(i), ps);
			params.append(bindVars.get(i)).append(",");
		}
		System.out.println(params.substring(0, params.lastIndexOf(",")));
		ir.rows = ps.executeUpdate();
		ResultSet generatedKeys = ps.getGeneratedKeys();
		while(generatedKeys.next()) {
			ir.generatedKey=generatedKeys.getInt(1);
		}
		return ir;
	}
	
	private List<Object> generateBindVars(Map<String, Object> valueMap,
			StringBuffer sb, StringBuffer values)
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
				if ("Calendar".equalsIgnoreCase(propType.getSimpleName())) {
					Calendar timeValue = (Calendar) value;
					String timeValueStr = df.format(timeValue.getTime());
					values.append("TO_DATE(?, 'YYYYMMDDHH24MISS'),");
					bindVars.add(timeValueStr);
				} else {
					values.append("?,");
					bindVars.add(value);
				}
			}
		}
		return bindVars;
	}	

	private <T> List<Object> generateBindVars(T bean, Map<String, String> propertyMapper,
			StringBuffer sb, StringBuffer values)
			throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		List<Object> bindVars = new ArrayList<>();		
		DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");		
		for (Iterator<String> iter = propertyMapper.keySet().iterator(); iter
				.hasNext();) {
			String propName = iter.next();
			Object value = PropertyUtils.getSimpleProperty(bean, propName);
			if (value != null) {
				sb.append(propertyMapper.get(propName)).append(",");
				Class<?> propType = propertyUtils.getPropertyType(bean,
						propName);
				if ("Calendar".equalsIgnoreCase(propType.getSimpleName())) {
					Calendar timeValue = (Calendar) value;
					String timeValueStr = df.format(timeValue.getTime());
					values.append("TO_DATE(?, 'YYYYMMDDHH24MISS'),");
					bindVars.add(timeValueStr);
				} else {
					values.append("?,");
					bindVars.add(value);
				}
			}
		}
		return bindVars;
	}
	


	public static class TestPerson {
		private int id;
		private String firstName;
		private String lastName;
		private int age;

		public String getCountry() {
			return country;
		}

		public void setCountry(String country) {
			this.country = country;
		}

		private String country;

		public String getFirstName() {
			return firstName;
		}

		public void setFirstName(String firstName) {
			this.firstName = firstName;
		}

		public String getLastName() {
			return lastName;
		}

		public void setLastName(String lastName) {
			this.lastName = lastName;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}
	}

	public static void main(String[] args) throws Throwable {
		Map<String, String> propMap = new HashMap<String, String>();
		propMap.put("firstName", "First_Name");
		propMap.put("lastName", "Last_Name");
		propMap.put("age", "Age");
		propMap.put("id", "person_id");
		propMap.put("country", "country");
		TestPerson tp = new JEasyDB.TestPerson();
		tp.setAge(39);
		tp.setFirstName("abc");
		tp.setLastName("value");
		tp.setId(345);
		tp.setCountry("India");
		//JEasyDB.getInstance().updateObject("TEST", tp, propMap, "id",
				//"country");

	}


	public List<Object> runStoredProc(String storedProc,
			LinkedHashMap<Object, PARAMTYPE> paramdefs) throws Throwable {
		List<Object> out = new ArrayList<Object>();
		CallableStatement cs = conn.prepareCall(storedProc);
		Map<Integer, Integer> outparams = registerStoredProcParams(paramdefs, cs);		
		cs.execute();
		for (Iterator<Integer> iter = outparams.keySet().iterator();iter.hasNext();) {
			int index = iter.next();
			out.add(determineOutValue(index, outparams.get(index), cs));
		}
		return out;
	}

	private Object determineOutValue(int index, int type, CallableStatement cs) throws SQLException {
		Object retValue=null;
		switch(type) {
			case Types.INTEGER:
				retValue = cs.getInt(index);
				break;
			case Types.BIGINT:
				retValue = cs.getLong(index);				
				break;
			case Types.DOUBLE:
				retValue = cs.getDouble(index);				
				break;
			case Types.FLOAT:
				retValue = cs.getFloat(index);				
				break;
			case Types.VARCHAR:
				retValue = cs.getString(index);
				break;
		}
		return retValue;
	}

}
