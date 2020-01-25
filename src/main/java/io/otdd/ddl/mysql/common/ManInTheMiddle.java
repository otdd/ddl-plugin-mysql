package io.otdd.ddl.mysql.common;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.mina.core.RuntimeIoException;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.mina.transport.socket.nio.NioSocketConnector;

import io.otdd.common.mima.codec.RawBytesCodecFactory;

public class ManInTheMiddle {

	private static final Logger LOGGER = LogManager.getLogger();

	FrontEndHandler handler = new FrontEndHandler();
	int port;
	String targetIp;
	int targetPort;

	private byte[] mockResp = null;

	private boolean recordResp = false;
	
	private byte[] recordedResp = null;

	private NioSocketAcceptor acceptor = null;

	static String BACK_END_SESSION_KEY = "BACK_END_SESSION_KEY";

	public ManInTheMiddle(int port,String targetIp,int targetPort){
		this.port = port;
		this.targetIp = targetIp;
		this.targetPort = targetPort;
	}

	public void start(){
		acceptor = new NioSocketAcceptor();
		acceptor.setReuseAddress(true);
		acceptor.getFilterChain().addLast( "logger", new LoggingFilter() );
		acceptor.getFilterChain().addLast("codec",
				new ProtocolCodecFilter(new RawBytesCodecFactory()));
		acceptor.setHandler( handler );
		try {
			acceptor.bind( new InetSocketAddress(port) );
		} catch (IOException e) {
			LOGGER.error("bind ManInTheMiddle failed on:"+port);
			e.printStackTrace();
		}
	}

	public void destroy(){
		try{
			acceptor.unbind();
			acceptor.dispose();
			handler.dispose();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	class FrontEndHandler extends IoHandlerAdapter{
		int index = 0;

		public Set<IoSession> frontSessions = new HashSet<IoSession>();

		@Override
		public void exceptionCaught( IoSession session, Throwable cause ) throws Exception
		{
			cause.printStackTrace();
		}

		public void dispose() {
			for(IoSession session:frontSessions){
				IoSession backEndSession = (IoSession)session.getAttribute(BACK_END_SESSION_KEY);
				if(backEndSession!=null){
					try{
						backEndSession.close();
					}
					catch(Exception e){
						e.printStackTrace();
					}
				}
				session.close();
			}
		}

		@Override
		public void messageReceived( IoSession session, Object message ) throws Exception
		{
			int port = ((InetSocketAddress)session.getLocalAddress()).getPort();
			String recv = new String((byte[])message);

			if(mockResp!=null){
				System.out.println("write back mocked bytes to client directly.");
				session.write(mockResp);
				mockResp = null;
			}
			else{
				LOGGER.info("packets received from front end, trying to write to backend:\n"+recv
						+"\nbytes:\n"+Arrays.toString((byte[])message));
				IoSession backEndSession = (IoSession)session.getAttribute(BACK_END_SESSION_KEY);
				if(backEndSession!=null){
					try{
						backEndSession.write(message);
					}
					catch(Exception e){
						session.close();
					}
				}
				else{
					session.close();
				}
			}
		}

		@Override
		public void sessionOpened(IoSession session) throws Exception {
			frontSessions.add(session);
			String remoteIp = ((InetSocketAddress)session.getRemoteAddress()).getAddress().getHostAddress();
			LOGGER.info("client connected. ip:"+remoteIp+" trying to connect to backend:"+targetIp+":"+targetPort);
			IoSession backEndSession = connect(session,targetIp,targetPort);
			if(backEndSession==null){
				LOGGER.info("backend not ready, close the connection.");
				session.close();
			}
			else{
				session.setAttribute(BACK_END_SESSION_KEY, backEndSession);
			}
		}

		public void sessionClosed(IoSession session) throws Exception {
			LOGGER.info("front end session closed");
			IoSession backEndSession = (IoSession)session.getAttribute(BACK_END_SESSION_KEY);
			if(backEndSession!=null){
				try{
					backEndSession.close();
				}
				catch(Exception e){
					e.printStackTrace();
				}
			}
			frontSessions.remove(session);
		}

		public IoSession connect(IoSession frontEndSession,String targetHost,Integer port) {

			BackEndHandler clientSessionHandler = new BackEndHandler(frontEndSession);

			NioSocketConnector connector = new NioSocketConnector();
			connector.setConnectTimeoutMillis(3000L);
			connector.getFilterChain().addLast("codec",
					new ProtocolCodecFilter(new RawBytesCodecFactory()));
			connector.setHandler(clientSessionHandler);
			try {
				ConnectFuture future = connector.connect(new InetSocketAddress(targetHost, port));
				future.awaitUninterruptibly();
				IoSession backEndSession = future.getSession();
				return backEndSession;
			} catch (RuntimeIoException e) {
				return null;
			}
		}
	}

	class BackEndHandler extends IoHandlerAdapter{

		IoSession frontEndSession;

		BackEndHandler(IoSession frontEndSession){
			this.frontEndSession = frontEndSession;
		}
		@Override
		public void messageReceived( IoSession session, Object message ) throws Exception
		{
			String recv = new String((byte[])message);
			LOGGER.info("packats received from backend,trying to write back to front end:\n"+recv
					+"\nbytes:\n"+Arrays.toString((byte[])message));
			if(recordResp){
				recordResp = false;
				recordedResp = (byte[])message;
			}
			frontEndSession.write(message);
		}

		public void sessionClosed(IoSession session) throws Exception {
			System.out.println("back end session closed");
		}
	}

	public byte[] getRecordedResp() {
		return recordedResp;
	}
	
	public void setRecordResp(boolean v) {
		recordResp = v;
	}

	public void setMockResp(byte[] bytes) {
		this.mockResp = bytes;
	}

}
