package io.otdd.ddl.mysql.common;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;

public class MysqlUtil {

	public static String getResultSetPrintStr(ResultSet resultSet) throws Exception {
		ResultSetMetaData rsmd = resultSet.getMetaData();
		int columnsNumber = rsmd.getColumnCount();
		StringBuilder sb = new StringBuilder();

		for (int i = 1; i <= columnsNumber; i++) {
			if (i > 1){
				sb.append(",  ");
			}
			sb.append(rsmd.getColumnName(i)+"<"+getSqlTypeName(rsmd.getColumnType(i))+">");
		}
		sb.append("\n");
		while (resultSet.next()) {
			for (int i = 1; i <= columnsNumber; i++) {
				if (i > 1){
					sb.append(",");
				}
				String columnValue = resultSet.getString(i);
				String type = getSqlTypeName(rsmd.getColumnType(i));
				if(columnValue!=null&&("BLOB".equalsIgnoreCase(type)
						||"CHAR".equalsIgnoreCase(type)
						||"DATE".equalsIgnoreCase(type)
						||"VARCHAR".equalsIgnoreCase(type)
						||"TIME".equalsIgnoreCase(type)
						||"TIMESTAMP".equalsIgnoreCase(type))
						){
					columnValue = "'"+columnValue+"'";
				}
				sb.append(columnValue);
			}
			sb.append("\n");
		}
		return sb.toString();
	}

	private static String readInputStream(InputStream stream) {
		try {
			return IOUtils.toString(stream);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String getSqlTypeName(int type) {
		switch(type){
		case java.sql.Types.BIGINT:
			return "BIGINT";
		case java.sql.Types.BIT:
			return "BIT";
		case java.sql.Types.BLOB:
			return "BLOB";
		case java.sql.Types.BOOLEAN:
			return "INT";
		case java.sql.Types.CHAR:
			return "CHAR";
		case java.sql.Types.DATE:
			return "DATE";
		case java.sql.Types.DECIMAL:
			return "DECIMAL";
		case java.sql.Types.DOUBLE:
			return "DOUBLE";
		case java.sql.Types.FLOAT:
			return "FLOAT";
		case java.sql.Types.INTEGER:
			return "INTEGER";
		case java.sql.Types.LONGNVARCHAR:
			return "VARCHAR";
		case java.sql.Types.LONGVARBINARY:
			return "VARCHAR";
		case java.sql.Types.NCHAR:
			return "VARCHAR";
		case java.sql.Types.NUMERIC:
			return "INT";
		case java.sql.Types.NVARCHAR:
			return "VARCHAR";
		case java.sql.Types.REAL:
			return "FLOAT";
		case java.sql.Types.ROWID:
			return "INT";
		case java.sql.Types.SMALLINT:
			return "SMALLINT";
		case java.sql.Types.TIME:
			return "TIME";
		case java.sql.Types.TIME_WITH_TIMEZONE:
			return "TIME";
		case java.sql.Types.TIMESTAMP:
			return "TIMESTAMP";
		case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
			return "TIMESTAMP";
		case java.sql.Types.TINYINT:
			return "TINYINT";
		case java.sql.Types.VARBINARY:
			return "VARCHAR";
		case java.sql.Types.VARCHAR:
			return "VARCHAR";
		case java.sql.Types.NULL:
			return "NULL";
		default:
			return "BLOB";
		}
	}
}
