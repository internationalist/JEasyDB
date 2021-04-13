package org.aguntuk.jeasydb;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public enum DynamicSQLUtil {
	instance;
	private final String EQUAL = "==";
	private final String NOTEQUAL = "!=";
	private final String pattern = "(?i)WHERE\\s*AND";

	public String generateInBinds(String sql, Map<String, Integer> bindCountMap) {
		if(bindCountMap != null) {
			for(Entry<String, Integer> entry:bindCountMap.entrySet()) {
				StringBuilder sb = new StringBuilder("(");
				for(int i = 0; i < entry.getValue(); i++) {
					sb.append("?,");
				}
				sb.replace(sb.length()-1, sb.length(), "");
				sb.append(")");
				sql=sql.replace("[[" + entry.getKey() + "]]", sb.toString());
			}
		}
		return sql;
	}
	
	public String formSQL(String sql, Map<String, Object> dynamicSQLargs) {
		return formSQL(sql, dynamicSQLargs, null);
	}
	
	public String formSQL(String sql, Map<String, Object> dynamicSQLargs, Map<String, Integer> bindCountMap) {
		String retValue;
		int ifIndex;
		String sqlPartA;
		String sqlPartB;
		String logicPart;
		String tempSQL = null;
		String key;
		String value;
		String clausePart;
		sql = generateInBinds(sql, bindCountMap);
		if(dynamicSQLargs == null) {
			//just create an empty map.
			dynamicSQLargs = new HashMap<String, Object>();
		}
		if((ifIndex = sql.indexOf("{if")) > -1) {
			String temp = sql.substring(ifIndex + 4, sql.indexOf("{/if}"));
			
			clausePart = temp.substring(temp.indexOf("}") + 1).trim();
			
			logicPart = temp.substring(0, temp.indexOf("}")).trim();
			
			sqlPartA = sql.substring(0, sql.indexOf("{if"));
			sqlPartB = sql.substring(sql.indexOf("{/if}") + 5);
			//tempSQL = sqlPartA + sqlPartB;
			//analyze the logic part.
			if(logicPart.indexOf(EQUAL) > -1) {
				key = logicPart.substring(0, logicPart.indexOf(EQUAL)).trim();
				value = logicPart.substring(logicPart.indexOf(EQUAL) + 2).trim();
				String realVal = convertToString(dynamicSQLargs.get(key));
				if(realVal.equalsIgnoreCase(value)) {
					tempSQL = modifySQL(dynamicSQLargs, sqlPartA, sqlPartB, key, clausePart);
				} else {
					tempSQL = sqlPartA + sqlPartB;
				}
			} else if(logicPart.indexOf(NOTEQUAL) > -1) {
				key = logicPart.substring(0, logicPart.indexOf(NOTEQUAL)).trim();
				value = logicPart.substring(logicPart.indexOf(NOTEQUAL) + 2).trim();
				
				if(value.equalsIgnoreCase("null")) {
					if(dynamicSQLargs.get(key) != null) {
						tempSQL = modifySQL(dynamicSQLargs, sqlPartA, sqlPartB, key, clausePart);						
					} else {
						tempSQL = sqlPartA + sqlPartB;						
					}
				} else {
					String realVal = convertToString(dynamicSQLargs.get(key));
					if(!realVal.equalsIgnoreCase(value)) {
						tempSQL = modifySQL(dynamicSQLargs, sqlPartA, sqlPartB, key, clausePart);
					} else {
						tempSQL = sqlPartA + sqlPartB;						
					}
				}
			}
		}
		
		if(tempSQL != null && (tempSQL.indexOf("{if") > -1 || tempSQL.indexOf("{when") > -1)) {
			retValue = formSQL(tempSQL, dynamicSQLargs);
		} else if(tempSQL != null) {
			retValue = tempSQL;
		} else {
			retValue = tempSQL;
		}
		//there is a scenario which can result in a WHERE followed by an AND. REPLACE THIS WITH WHERE
		retValue = retValue.replaceAll(pattern, " WHERE ");
		return retValue;
	}

	private String modifySQL(Map<String, Object> dynamicSQLargs, 
			String sqlPartA, String sqlPartB, String key, String clausePart) {
		if(sqlPartA.toUpperCase().indexOf("WHERE") > -1) {
			sqlPartA = sqlPartA.trim() + " AND " + clausePart;
		} else {
			sqlPartA = sqlPartA.trim() + " WHERE " + clausePart;
		}
		return sqlPartA + sqlPartB;
	}
	
	private String convertToString(Object value) {
		String retValue = String.valueOf(value);
		return retValue;
	}
	
	public static void main(String[] args) {
		//String test = "select * from testtab where first_name = ? {{if address != null}} UPPER(address) != UPPER(?) {{/if}} {{if state != null}} state = ? {{/if}}  and last_name = ?";
		String test = "select * from testtab where {if address != null} UPPER(address) != UPPER(?) {/if} AND city = 'Shelton {if state != null} state = ? {/if}";
		//String test = "select * from testtab where first_name = ? and last_name = ?";
		Map<String, Object> dynamicSQLArgs = new HashMap<String, Object>();
		dynamicSQLArgs.put("address", "800 Connecticut Ave");
		dynamicSQLArgs.put("state", "CT");
		
		String ret = DynamicSQLUtil.instance.formSQL(test, dynamicSQLArgs);
		//String ret = DynamicSQLUtil.instance.formSQL(test, null);
		System.out.println(ret);
	}	
}