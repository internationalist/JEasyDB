package org.aguntuk.jeasydb.v2;

import static org.aguntuk.jeasydb.v2.utils.GeneralUtils.bindPreparedStatement;
import static org.aguntuk.jeasydb.v2.utils.GeneralUtils.mapBeanProperty;
import static org.aguntuk.jeasydb.v2.utils.GeneralUtils.populateBeanFromSQL;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataObjectReader extends AbstractData {
	
	public DataObjectReader() {
		super();
	}
	
	public <T> List<T> load(String sql, Class<T> bean,
			  Object... sqlArgs) throws Exception {
		return load(sql, bean, null, sqlArgs);
	}
	
	public <T> List<T> load(String sql, Class<T> bean,
			Map<String, String> propertyMapper, Object... sqlArgs)
			throws Exception {
		BiDiMap<String, String> bm = null;
		if (propertyMapper != null) {
			bm = new BiDiMap<String, String>(propertyMapper);
		}		
		List<T> retValue = new ArrayList<T>();
		PreparedStatement stmt = conn.prepareStatement(sql);
		
		if (sqlArgs != null && sqlArgs.length > 0) {
			for (int i = 0; i < sqlArgs.length; i++) {
				bindPreparedStatement(i, sqlArgs[i], stmt);
			}
		}
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
				T bInstance = bean.getDeclaredConstructor().newInstance();
				populateBeanFromSQL(rs, bInstance, bm, mapBeanProperty(bInstance));
				retValue.add(bInstance);
		}
		return retValue;
	}	
}
