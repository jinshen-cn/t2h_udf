package com.ebay.t2h.util;

import java.io.IOException;

import ch.ethz.ssh2.ChannelCondition;
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;

public class RemoteShellExec {
	private Connection conn;
	private String ip;
	private String user;
	private String passwd;
	public static int count = 0;
	
	//private String charset = Charset.defaultCharset().toString();
	
	public RemoteShellExec(String ip, String user, String passwd) {
		this.ip = ip;
		this.user = user;
		this.passwd = passwd;
	}
	
	private boolean login() throws IOException {
		conn = new Connection(ip);
		conn.connect();
		return conn.authenticateWithPassword(user, passwd);
	}
	
//	private String processStream(InputStream in, String charset) throws Exception {
//		byte[] buffer  = new byte[1024];
//		StringBuffer sb = new StringBuffer();
//		while (in.read(buffer) != -1) {
//			sb.append(new String(buffer,charset));
//		}
//		return sb.toString();
//	}
	
	public int exec(String command) throws Exception {
		int ret = -1;
		try {
			if(login()) {
				++count;
				Session session = conn.openSession();
				session.execCommand(command);
				session.waitForCondition(ChannelCondition.EXIT_STATUS, 0);
				ret = session.getExitStatus();
			} else {
				throw new Exception("connection hadoop server failed!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
		
		return ret;
	}
	
	public int execHiveSQL(String hiveSQL) throws Exception {
		int ret = -1;
		hiveSQL = "/usr/local/hive/bin/hive -e " + "\"" + hiveSQL + "\"";
		
//		InputStream stdout = null;
//		InputStream stderr = null;
//		String outStr = null;
//		String outErr = null;
		try {
			if (login()) {
				++count;
				Session session = conn.openSession();
				session.execCommand(hiveSQL);
//				stdout = new StreamGobbler(session.getStdout());
//				outStr = processStream(stdout, charset);
//				
//				stderr = new StreamGobbler(session.getStderr());
//				outErr = processStream(stderr, charset);
				session.waitForCondition(ChannelCondition.EXIT_STATUS, 0);
				ret = session.getExitStatus();
			} else {
				throw new Exception("connection hadoop server failed!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
		
		return ret;
	}

}
