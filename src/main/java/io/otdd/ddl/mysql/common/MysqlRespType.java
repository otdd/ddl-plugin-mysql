package io.otdd.ddl.mysql.common;

public enum MysqlRespType {
	RESP_TYPE_RESULTSET("text resultset"),
	RESP_TYPE_OK("insert/update/delete response"),
	RESP_TYPE_PREPARED_STATEMENT("binary resultset(for prepared statement)"),
	UNKNONW("unknown");

	private String desc;

	private MysqlRespType(String d){
		desc = d;
	}
	
	public boolean equalsDesc(String d) {
        return desc.equalsIgnoreCase(d);
    }

	public String toString() {
		return this.desc;
	}
}
