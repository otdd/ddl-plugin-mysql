package io.otdd.ddl.mysql.encoder;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import io.otdd.common.util.PortUtil;
import io.otdd.ddl.mysql.MysqlDDLCodecFactoryPlugin;
import io.otdd.ddl.mysql.common.ManInTheMiddle;
import io.otdd.ddl.mysql.common.MysqlRespType;
import io.otdd.ddl.mysql.common.PacketHeader;
import io.otdd.ddl.mysql.common.ProtocolUtil;
import io.otdd.ddl.mysql.common.SettingsUtil;
import io.otdd.ddl.plugin.DDLEncoder;

public class MysqlDDLEncoder  implements DDLEncoder {

	private int mainInTheMiddlePort;
	private ManInTheMiddle manInTheMiddle;

	private static final Logger LOGGER = LogManager.getLogger();

	private static final String GEN_TABLE_NAME = "otdd_data_table";

	public boolean init(int dbPort){
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(
				MysqlDDLCodecFactoryPlugin.classLoader!=null?MysqlDDLCodecFactoryPlugin.classLoader:classLoader);
		try {
			mainInTheMiddlePort = PortUtil.getNextAvailablePort(40000,60000-40000);
			manInTheMiddle = new ManInTheMiddle(mainInTheMiddlePort,"127.0.0.1",dbPort);
			manInTheMiddle.start();
			LOGGER.debug("internal man in the middle started.");
			LOGGER.info("MysqlDDLDecoder inited.");
			return true;
		}catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		finally{
			Thread.currentThread().setContextClassLoader(classLoader);
		}
	}

	@Override
	public byte[] encodeRequest(String ddl, Map<String, String> protocolSettings) {
		return null;
	}

	@Override
	public byte[] encodeResponse(String ddl, Map<String, String> protocolSettings) {
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(
				MysqlDDLCodecFactoryPlugin.classLoader!=null?MysqlDDLCodecFactoryPlugin.classLoader:classLoader);
		Connection conn = null;
		Statement statement = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		try{
			try{
				Class.forName("com.mysql.jdbc.Driver");
				conn = DriverManager.getConnection("jdbc:mysql://"+"127.0.0.1"+":"+mainInTheMiddlePort+"/"+"test"+"?useUnicode=true&characterEncoding=utf-8&"
						+ "user="+"root"+"&password="+""+"&useSSL=false&useServerPrepStmts=true");
				if(!conn.isValid(0)){
					return null;
				}
			}
			catch(Exception e){
				e.printStackTrace();
				return null;
			}

			MysqlRespType type = SettingsUtil.getRespType(protocolSettings);
			byte[] ret = null;
			if(!prepareTable(conn,ddl,type)){
				return null;
			}
			switch(type){
			case RESP_TYPE_RESULTSET:
				statement = conn.createStatement();
				manInTheMiddle.setRecordResp(true);
				rs = statement.executeQuery("select * from "+GEN_TABLE_NAME);
				while(rs.next()){
					//
				}
				return manInTheMiddle.getRecordedResp();
			case RESP_TYPE_PREPARED_STATEMENT:
				preparedStatement = conn.prepareStatement("select * from "+GEN_TABLE_NAME+" where 1=?");
				preparedStatement.setInt(1, 1);
				manInTheMiddle.setRecordResp(true);
				rs = preparedStatement.executeQuery();
				while(rs.next()){
					//
				}
				return manInTheMiddle.getRecordedResp();
			case RESP_TYPE_OK:
				statement = conn.createStatement();
				manInTheMiddle.setRecordResp(true);
				statement.executeUpdate("delete from "+GEN_TABLE_NAME);
				String[] lines = ddl.split("\n");
				if(lines.length<2){
					return null;
				}
				int rowCnt = Integer.parseInt(lines[1]);
				return fixEffectedRowsCnt(manInTheMiddle.getRecordedResp(),rowCnt);
			default:
				break;
			}
			return ret;
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			if(rs!=null){
				try {
					rs.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if(statement!=null){
				try {
					statement.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if(preparedStatement!=null){
				try {
					preparedStatement.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if(conn!=null){
				try {
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			Thread.currentThread().setContextClassLoader(classLoader);
		}
		return null;
	}

	//https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
	private byte[] fixEffectedRowsCnt(byte[] bytes,int cnt) {
		if(bytes==null){
			return null;
		}
		//substute the effected rows field.
		ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
		ProtocolUtil.network_mysqld_proto_append_lenenc_int(byteArray, cnt);
		byte[] effectedRowsBytes = byteArray.toByteArray();
		byte[] ret = new byte[bytes.length-1+effectedRowsBytes.length];
		System.arraycopy(bytes, 0, ret, 0, 5);
		System.arraycopy(effectedRowsBytes,0,ret,5,effectedRowsBytes.length);
		System.arraycopy(bytes, 6, ret, 5+effectedRowsBytes.length, bytes.length-6);
		PacketHeader header = PacketHeader.fromBytes(bytes);
		
		//fix the header length;
		header.payload_length = ret.length-4;
		byte[] headerBytes = header.getBytes();
		if(headerBytes.length!=4){
			return bytes;
		}
		for(int i=0;i<4;i++){
			ret[i] = headerBytes[i];
		}
		return ret;
	}

	private boolean prepareTable(Connection conn, String ddl,MysqlRespType respType) {
		Statement stmt = null;
		try{
			stmt = conn.createStatement();
			stmt.executeUpdate("DROP TABLE IF EXISTS " + GEN_TABLE_NAME + ";");

			if(respType==MysqlRespType.RESP_TYPE_OK){
				stmt.executeUpdate("CREATE TABLE " + GEN_TABLE_NAME + " (id INTEGER);");
				stmt.executeUpdate("INSERT INTO " + GEN_TABLE_NAME + " (`id`) values(1)");
				return true;
			}
			String[] lines = ddl.split("\n");
			if(lines.length<1){
				return false;
			}
			String tableLine = lines[0];
			String fields[] = tableLine.split(",");
			if(fields.length==0){
				return false;
			}
			StringBuilder createTableSb = new StringBuilder();
			StringBuilder insertTableSb = new StringBuilder();
			for(String field:fields){
				String name = field.substring(0, field.indexOf("<"));
				String type = field.substring(field.indexOf("<")+1, field.indexOf(">"));
				if(type.trim().equalsIgnoreCase("varchar")){
					type = "VARCHAR(1024)";
				}
				createTableSb.append("`"+name.trim()+"`"+" "+type.trim()+",");
				insertTableSb.append("`"+name.trim()+"`,");
			}
			String createFieldsStr = createTableSb.substring(0,createTableSb.length()-1);
			String insertFieldsStr = insertTableSb.substring(0,insertTableSb.length()-1);

			//prepare the schema
			String createTable = "CREATE TABLE " + GEN_TABLE_NAME + " ("+createFieldsStr+")";
			stmt.executeUpdate(createTable);

			//prepare the data
			String insertTable = "INSERT INTO " + GEN_TABLE_NAME + " ("+insertFieldsStr+") VALUES ";
			for(int i=1;i<lines.length;i++){
				String[] values = lines[i].split(",");
				StringBuilder valueSb = new StringBuilder();
				for(String value:values){
					valueSb.append(value+",");
				}
				insertTable += "("+valueSb.substring(0, valueSb.length()-1)+"),";
			}
			insertTable = insertTable.substring(0, insertTable.length()-1);
			stmt.executeUpdate(insertTable);
			return true;
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			try {
				if(stmt!=null){
					stmt.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return false;
	}

	public void destroy() {
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(
				MysqlDDLCodecFactoryPlugin.classLoader!=null?MysqlDDLCodecFactoryPlugin.classLoader:classLoader);
		try {
			if(manInTheMiddle!=null){
				manInTheMiddle.destroy();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally{
			Thread.currentThread().setContextClassLoader(classLoader);
		}
	}

	public static void main(String args[]){
		try{
			int dbPort = PortUtil.getNextAvailablePort(40000,60000-40000);
			DBConfigurationBuilder configBuilder = DBConfigurationBuilder.newBuilder();
			configBuilder.setPort(dbPort);
			configBuilder.addArg("--user=root");
			DB db = DB.newEmbeddedDB(configBuilder.build());
			db.start();
			MysqlDDLEncoder encoder = new MysqlDDLEncoder();
			encoder.init(dbPort);
//			String ddl = "id<INTEGER>,  name<VARCHAR>,  type<INTEGER>,  insert_time<TIMESTAMP>\n1,'aaa',222,'2018-07-19 18:12:21'\n2,'bbb',333,'2018-07-19 18:12:33'";
			String ddl = "effected_rows\n65537";
			Map<String, String> protocolSettings = new HashMap<String,String>();
//			protocolSettings.put("response type", "binary resultset(for prepared statement)");
			protocolSettings.put("response type", "insert/update/delete response");
			byte[] bytes = encoder.encodeResponse(ddl, protocolSettings);
			System.out.println("bytes:"+Arrays.toString(bytes));
			System.out.println(new String(bytes));
		}
		catch(Exception e){

		}
	}
}
