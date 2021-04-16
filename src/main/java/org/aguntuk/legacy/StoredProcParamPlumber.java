package org.aguntuk.legacy;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.aguntuk.legacy.JEasyDB.PARAMTYPE;

public enum StoredProcParamPlumber {
	instance;
	public Map<Integer, Integer> registerParams(Object value, PARAMTYPE pType, CallableStatement cs, int index) throws SQLException {
		Map<Integer, Integer> outIndexMap = null;
		if (value != null) {
			String name = value.getClass().getSimpleName();
			switch(name) {
				case "int":
					outIndexMap = registerIntParam(value, pType, cs, index);
					break;
				case "Integer":
					outIndexMap = registerIntParam(value, pType, cs, index);					
					break;
				case "float":
					outIndexMap = registerFloatParam(value, pType, cs, index);
					break;
				case "Float":
					outIndexMap = registerFloatParam(value, pType, cs, index);
					break;
				case "double":
					outIndexMap = registerDoubleParam(value, pType, cs, index);
					break;
				case "Double":
					outIndexMap = registerDoubleParam(value, pType, cs, index);					
					break;
				case "long":
					outIndexMap = registerLongParam(value, pType, cs, index);					
					break;
				case "Long":
					outIndexMap = registerLongParam(value, pType, cs, index);
					break;
				case "String":
					outIndexMap = registerStringParam(value, pType, cs, index);
					break;					
			}
		}
		return outIndexMap;
	}

	private Map<Integer, Integer> registerIntParam(Object value, PARAMTYPE pType, CallableStatement cs, int index) throws SQLException {
		Map<Integer, Integer> out = new HashMap<Integer, Integer>();		
		int paramType = Types.INTEGER;
		switch(pType) {
			case IN:
				cs.setInt(index, (Integer)value);
				break;
			case INOUT:
				cs.registerOutParameter(index, paramType);
				cs.setInt(index, (Integer)value);
				out.put(index, paramType);				
				break;
			case OUT:
				cs.registerOutParameter(index, paramType);
				out.put(index, paramType);				
				break;
		}
		return out;
	}
	
	private Map<Integer, Integer> registerFloatParam(Object value, PARAMTYPE pType, CallableStatement cs, int index) throws SQLException {
		Map<Integer, Integer> out = new HashMap<Integer, Integer>();
		int paramType = Types.FLOAT;
		switch(pType) {
			case IN:
				cs.setFloat(index, (Float)value);
				break;
			case INOUT:
				cs.registerOutParameter(index, paramType);
				cs.setFloat(index, (Float)value);
				out.put(index, paramType);
				break;
			case OUT:
				cs.registerOutParameter(index, paramType);
				out.put(index, paramType);				
				break;
		}
		return out;
	}
	
	private Map<Integer, Integer> registerLongParam(Object value, PARAMTYPE pType, CallableStatement cs, int index) throws SQLException {
		Map<Integer, Integer> out = new HashMap<Integer, Integer>();		
		int paramType = Types.BIGINT;
		switch(pType) {
			case IN:
				cs.setLong(index, (Long)value);
				break;
			case INOUT:
				cs.registerOutParameter(index, paramType);
				cs.setLong(index, (Long)value);
				out.put(index, paramType);				
				break;
			case OUT:
				cs.registerOutParameter(index, paramType);
				out.put(index, paramType);				
				break;
		}
		return out;
	}
	
	private Map<Integer, Integer> registerDoubleParam(Object value, PARAMTYPE pType, CallableStatement cs, int index) throws SQLException {
		Map<Integer, Integer> out = new HashMap<Integer, Integer>();		
		int paramType = Types.DOUBLE;
		switch(pType) {
			case IN:
				cs.setDouble(index, (Double)value);
				break;
			case INOUT:
				cs.registerOutParameter(index, paramType);
				cs.setDouble(index, (Double)value);
				out.put(index, paramType);				
				break;
			case OUT:
				cs.registerOutParameter(index, paramType);
				out.put(index, paramType);				
				break;
		}
		return out;
	}
	
	private Map<Integer, Integer> registerStringParam(Object value, PARAMTYPE pType, CallableStatement cs, int index) throws SQLException {
		Map<Integer, Integer> out = new HashMap<Integer, Integer>();		
		int paramType = Types.VARCHAR;
		switch(pType) {
			case IN:
				cs.setString(index, (String)value);
				break;
			case INOUT:
				cs.registerOutParameter(index, paramType);
				cs.setString(index, (String)value);
				out.put(index, paramType);				
				break;
			case OUT:
				cs.registerOutParameter(index, paramType);
				out.put(index, paramType);				
				break;
		}
		return out;
	}	
}
