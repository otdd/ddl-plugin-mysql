package io.otdd.ddl.mysql.decoder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import io.otdd.common.util.PortUtil;
import io.otdd.ddl.mysql.MysqlDDLCodecFactoryPlugin;
import io.otdd.ddl.mysql.common.ManInTheMiddle;
import io.otdd.ddl.mysql.common.MysqlRespType;
import io.otdd.ddl.mysql.common.MysqlUtil;
import io.otdd.ddl.mysql.common.ProtocolUtil;
import io.otdd.ddl.mysql.common.SettingsUtil;
import io.otdd.ddl.plugin.DDLDecoder;

public class MysqlDDLDecoder implements DDLDecoder {

	private int mainInTheMiddlePort;
	private ManInTheMiddle manInTheMiddle;

	private static final Logger LOGGER = LogManager.getLogger();

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
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		finally{
			Thread.currentThread().setContextClassLoader(classLoader);
		}
	}

	public void destroy(){
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

	public String decodeRequest(byte[] bytes,Map<String,String> protocolSettings) {
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(
				MysqlDDLCodecFactoryPlugin.classLoader!=null?MysqlDDLCodecFactoryPlugin.classLoader:classLoader);		
		try{

			if(!ProtocolUtil.validMysqlPacket(bytes, 0, bytes.length)){
				return null;
			}

			//no need to decode request.
			return null;
		}
		finally{
			Thread.currentThread().setContextClassLoader(classLoader);
		}
	}

	public String decodeResponse(byte[] bytes,Map<String,String> protocolSettings) {
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(
				MysqlDDLCodecFactoryPlugin.classLoader!=null?MysqlDDLCodecFactoryPlugin.classLoader:classLoader);

		if(!ProtocolUtil.validMysqlPacket(bytes, 0, bytes.length)){
			return null;
		}

		Connection conn = null;
		Statement statement = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
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
			String str = null;
			switch(type){
			case RESP_TYPE_RESULTSET:
				try{
					statement = conn.createStatement();
					manInTheMiddle.setMockResp(bytes);
					resultSet = statement.executeQuery("select * from mock");
					str = MysqlUtil.getResultSetPrintStr(resultSet);
				}
				catch(Exception e){
				}
				break;
			case RESP_TYPE_PREPARED_STATEMENT:
				try{
					//must be a valid prepared statement.
					preparedStatement = conn.prepareStatement("select * from mysql.user where max_user_connections > ?");
					preparedStatement.setInt(1, 0);
					manInTheMiddle.setMockResp(bytes);
					resultSet = preparedStatement.executeQuery();
					str = MysqlUtil.getResultSetPrintStr(resultSet);
				}
				catch(Exception e){

				}
				break;
			case RESP_TYPE_OK://update / delete / insert share the same RESP_TYPE_OK type.
				//https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
				statement = conn.createStatement();
				manInTheMiddle.setMockResp(bytes);
				int updateCnt = statement.executeUpdate("update mock set id = id");
				str = "affected_rows\n"+updateCnt;
				try{
					statement.close();
				}
				catch(Exception e){
					e.printStackTrace();
				}
				break;
			default:
				break;
			}
			return str;
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			if(statement!=null){
				try {
					statement.close();
				} catch (Exception e) {
				}
			}
			if(preparedStatement!=null){
				try {
					preparedStatement.close();
				} catch (Exception e) {
				}
			}
			if(resultSet!=null){
				try {
					resultSet.close();
				} catch (Exception e) {
				}
			}
			if(conn!=null){
				try {
					conn.close();
				} catch (Exception e) {
				}
			}
			Thread.currentThread().setContextClassLoader(classLoader);
		}
		return null;
	}

	private MysqlRespType guessType(byte[] bytes) {
		if(bytes.length<5){
			return MysqlRespType.UNKNONW;
		}
		byte b = bytes[4];
		if(b==0){
			int packetNum = ProtocolUtil.getNumOfMysqlPacketNum(bytes, 0, bytes.length);
			/*
			 * currently the OK_Packet and Prepared Response Packet cannot be well distinguished from the bytes itself.
			 *  
			 * https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
			 * https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html
			 * but OK_Packet consist less than 2 packets while Prepared Response may in big chance consist of more than 2 packets. 
			 */
			if(packetNum<=2){
				return MysqlRespType.RESP_TYPE_OK;
			}
			else{
				return MysqlRespType.RESP_TYPE_PREPARED_STATEMENT;
			}
		}
		else if(b==0xff||b==0xfb){//for ERR and GET_MORE_CLIENT_DATA https://dev.mysql.com/doc/internals/en/com-query-response.html
			return MysqlRespType.UNKNONW;
		}
		return MysqlRespType.RESP_TYPE_RESULTSET;
	}

	public static void main(String args[]){
		try{
			int dbPort = PortUtil.getNextAvailablePort(40000,60000-40000);
			DBConfigurationBuilder configBuilder = DBConfigurationBuilder.newBuilder();
			configBuilder.setPort(dbPort);
			configBuilder.addArg("--user=root");
			DB db = DB.newEmbeddedDB(configBuilder.build());
			db.start();
			MysqlDDLDecoder decoder = new MysqlDDLDecoder();
			decoder.init(dbPort);
			//		byte[] selectResp = new byte[]{
			//				1, 0, 0, 1, 4, 38, 0, 0, 2, 3, 100, 101, 102, 4, 111, 116, 100, 100, 4, 116, 101, 115, 116, 4, 116, 101, 115, 116, 2, 105, 100, 2, 105, 100, 12, 63, 0, 11, 0, 0, 0, 3, 3, 80, 0, 0, 0, 42, 0, 0, 3, 3, 100, 101, 102, 4, 111, 116, 100, 100, 4, 116, 101, 115, 116, 4, 116, 101, 115, 116, 4, 110, 97, 109, 101, 4, 110, 97, 109, 101, 12, 33, 0, -121, 0, 0, 0, -3, 0, 0, 0, 0, 0, 42, 0, 0, 4, 3, 100, 101, 102, 4, 111, 116, 100, 100, 4, 116, 101, 115, 116, 4, 116, 101, 115, 116, 4, 116, 121, 112, 101, 4, 116, 121, 112, 101, 12, 63, 0, 11, 0, 0, 0, 3, 0, 0, 0, 0, 0, 56, 0, 0, 5, 3, 100, 101, 102, 4, 111, 116, 100, 100, 4, 116, 101, 115, 116, 4, 116, 101, 115, 116, 11, 105, 110, 115, 101, 114, 116, 95, 116, 105, 109, 101, 11, 105, 110, 115, 101, 114, 116, 95, 116, 105, 109, 101, 12, 63, 0, 19, 0, 0, 0, 12, -128, 0, 0, 0, 0, 30, 0, 0, 6, 1, 49, 3, 97, 97, 97, 3, 50, 50, 50, 19, 50, 48, 49, 56, 45, 48, 55, 45, 49, 57, 32, 49, 56, 58, 49, 50, 58, 50, 49, 30, 0, 0, 7, 1, 50, 3, 98, 98, 98, 3, 51, 51, 51, 19, 50, 48, 49, 56, 45, 48, 55, 45, 49, 57, 32, 49, 56, 58, 49, 50, 58, 51, 51, 7, 0, 0, 8, -2, 0, 0, 34, 0, 0, 0
			//		};
			//		System.out.print(decoder.decodeResponse(selectResp));

			//		byte[] updateResp = new byte[]{
			//				48, 0, 0, 1, 0, 1, 0, 2, 0, 0, 0, 40, 82, 111, 119, 115, 32, 109, 97, 116, 99, 104, 101, 100, 58, 32, 49, 32, 32, 67, 104, 97, 110, 103, 101, 100, 58, 32, 48, 32, 32, 87, 97, 114, 110, 105, 110, 103, 115, 58, 32, 48
			//		};
			//		System.out.print(decoder.decodeResponse(updateResp));

			//		byte[] insertResp = new byte[]{
			//				7, 0, 0, 1, 0, 1, 0, 2, 0, 0, 0
			//		};
			//		System.out.print(decoder.decodeResponse(insertResp));

			//		byte[] deleteResp = new byte[]{
			//				7, 0, 0, 1, 0, 1, 0, 2, 0, 0, 07, 0, 0, 1, 0, 1, 0, 2, 0, 0, 0
			//		};
			//		System.out.print(decoder.decodeResponse(deleteResp));

			//		byte[] preparedSelectResp = new byte[]{
			//				12, 0, 0, 1, 0, -87, 2, 0, 0, 4, 0, 1, 0, 0, 0, 0, 23, 0, 0, 2, 3, 100, 101, 102, 0, 0, 0, 1, 63, 0, 12, 63, 0, 0, 0, 0, 0, -3, -128, 0, 0, 0, 0, 38, 0, 0, 3, 3, 100, 101, 102, 4, 111, 116, 100, 100, 4, 116, 101, 115, 116, 4, 116, 101, 115, 116, 2, 105, 100, 2, 105, 100, 12, 63, 0, 11, 0, 0, 0, 3, 3, 80, 0, 0, 0, 42, 0, 0, 4, 3, 100, 101, 102, 4, 111, 116, 100, 100, 4, 116, 101, 115, 116, 4, 116, 101, 115, 116, 4, 110, 97, 109, 101, 4, 110, 97, 109, 101, 12, 33, 0, -121, 0, 0, 0, -3, 0, 0, 0, 0, 0, 42, 0, 0, 5, 3, 100, 101, 102, 4, 111, 116, 100, 100, 4, 116, 101, 115, 116, 4, 116, 101, 115, 116, 4, 116, 121, 112, 101, 4, 116, 121, 112, 101, 12, 63, 0, 11, 0, 0, 0, 3, 0, 0, 0, 0, 0, 56, 0, 0, 6, 3, 100, 101, 102, 4, 111, 116, 100, 100, 4, 116, 101, 115, 116, 4, 116, 101, 115, 116, 11, 105, 110, 115, 101, 114, 116, 95, 116, 105, 109, 101, 11, 105, 110, 115, 101, 114, 116, 95, 116, 105, 109, 101, 12, 63, 0, 19, 0, 0, 0, 12, -128, 0, 0, 0, 0
			//		};
			//		System.out.print(decoder.decodeResponse(preparedSelectResp));

			byte[] preparedSelectExecResp = new byte[]{
					1, 0, 0, 1, 4, 38, 0, 0, 2, 3, 100, 101, 102, 4, 111, 116, 100, 100, 4, 116, 101, 115, 116, 4, 116, 101, 115, 116, 2, 105, 100, 2, 105, 100, 12, 63, 0, 11, 0, 0, 0, 3, 3, 80, 0, 0, 0, 42, 0, 0, 3, 3, 100, 101, 102, 4, 111, 116, 100, 100, 4, 116, 101, 115, 116, 4, 116, 101, 115, 116, 4, 110, 97, 109, 101, 4, 110, 97, 109, 101, 12, 33, 0, -121, 0, 0, 0, -3, 0, 0, 0, 0, 0, 42, 0, 0, 4, 3, 100, 101, 102, 4, 111, 116, 100, 100, 4, 116, 101, 115, 116, 4, 116, 101, 115, 116, 4, 116, 121, 112, 101, 4, 116, 121, 112, 101, 12, 63, 0, 11, 0, 0, 0, 3, 0, 0, 0, 0, 0, 56, 0, 0, 5, 3, 100, 101, 102, 4, 111, 116, 100, 100, 4, 116, 101, 115, 116, 4, 116, 101, 115, 116, 11, 105, 110, 115, 101, 114, 116, 95, 116, 105, 109, 101, 11, 105, 110, 115, 101, 114, 116, 95, 116, 105, 109, 101, 12, 63, 0, 19, 0, 0, 0, 12, -128, 0, 0, 0, 0, 22, 0, 0, 6, 0, 0, 1, 0, 0, 0, 3, 97, 97, 97, -34, 0, 0, 0, 7, -30, 7, 7, 19, 18, 12, 21, 22, 0, 0, 7, 0, 0, 2, 0, 0, 0, 3, 98, 98, 98, 77, 1, 0, 0, 7, -30, 7, 7, 19, 18, 12, 33, 7, 0, 0, 8, -2, 0, 0, 2, 0, 0, 0
			};
			System.out.print(decoder.decodeResponse(preparedSelectExecResp,null));
		}
		catch(Exception e){

		}
	}

}
