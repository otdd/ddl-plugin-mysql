package io.otdd.ddl.mysql;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.pf4j.Extension;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import io.otdd.common.util.PortUtil;
import io.otdd.ddl.mysql.common.SettingsUtil;
import io.otdd.ddl.mysql.decoder.MysqlDDLDecoder;
import io.otdd.ddl.mysql.encoder.MysqlDDLEncoder;
import io.otdd.ddl.plugin.DDLCodecFactory;
import io.otdd.ddl.plugin.DDLDecoder;
import io.otdd.ddl.plugin.DDLEncoder;

public class MysqlDDLCodecFactoryPlugin extends Plugin {

	public static ClassLoader classLoader = null;

	public MysqlDDLCodecFactoryPlugin(PluginWrapper wrapper) {
		super(wrapper);
		MysqlDDLCodecFactoryPlugin.classLoader = wrapper.getPluginClassLoader();
	}

	@Extension
	public static class MysqlDDLCodecFactory implements DDLCodecFactory{

		private static final Logger LOGGER = LogManager.getLogger();

		private MysqlDDLDecoder decoder = new MysqlDDLDecoder();
		private MysqlDDLEncoder encoder = new MysqlDDLEncoder();
		private DB db;

		public Map<String, String> getReqProtocolSettings() {
			return SettingsUtil.getReqProtocolSettings();
		}

		public Map<String, String> getRespProtocolSettings() {
			return SettingsUtil.getRespProtocolSettings();
		}

		public boolean init(Map<String,String> reqProtocolSettings,Map<String,String> respProtocolSettings) {
			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			Thread.currentThread().setContextClassLoader(
					MysqlDDLCodecFactoryPlugin.classLoader!=null?MysqlDDLCodecFactoryPlugin.classLoader:classLoader);
			try{
				SettingsUtil.setDefaultSettings(reqProtocolSettings,respProtocolSettings);
				try{
					int dbPort = PortUtil.getNextAvailablePort(40000,60000-40000);
					DBConfigurationBuilder configBuilder = DBConfigurationBuilder.newBuilder();
					configBuilder.setPort(dbPort);
					configBuilder.addArg("--user=root");
					db = DB.newEmbeddedDB(configBuilder.build());
					db.start();
					LOGGER.debug("internal MariaDB started.");
					if(!decoder.init(dbPort)){
						LOGGER.debug("failed to init mysql decoder.");
						return false;
					}
					if(!encoder.init(dbPort)){
						LOGGER.debug("failed to init mysql encoder.");
						return false;
					}
				}
				catch(Exception e){

				}
				return true;
			}
			finally{
				Thread.currentThread().setContextClassLoader(classLoader);
			}
		}

		@Override
		public boolean updateSettings(Map<String, String> reqProtocolSettings,
				Map<String, String> respProtocolSettings) {
			SettingsUtil.setDefaultSettings(reqProtocolSettings,respProtocolSettings);
			return true;
		}
		
		public void destroy() {
			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			Thread.currentThread().setContextClassLoader(
					MysqlDDLCodecFactoryPlugin.classLoader!=null?MysqlDDLCodecFactoryPlugin.classLoader:classLoader);
			try{
				decoder.destroy();
				encoder.destroy();
				if(db!=null){
					try {
						db.stop();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			finally{
				Thread.currentThread().setContextClassLoader(classLoader);
			}
		}

		public DDLDecoder getDecoder() {
			return decoder;
		}

		public DDLEncoder getEncoder() {
			return encoder;
		}

		public String getProtocolName() {
			return "mysql";
		}

		public String getPluginName() {
			return "mysql/5.7(mariadb/10.2.11)";
		}

	}

}
