package org.aguntuk.legacy;

public enum DBSpecificSQLFrags {
	instance;
	public String getMergeFrag(DBTypes dbtype) {
		String mergeFrag = null;
		switch(dbtype) {
		case MYSQL:
			mergeFrag = "INSERT IGNORE";
			break;
		case ORACLE:
			mergeFrag = "MERGE";
			break;
		case SQLITE:
			mergeFrag = "INSERT OR REPLACE";
			break;
		default:
			mergeFrag = "MERGE";
		}
		return mergeFrag;
	}
}
