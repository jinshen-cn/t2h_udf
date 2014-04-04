package com.ebay.t2h;
import com.ebay.t2h.util.DomParseXML;
import com.ebay.t2h.util.RemoteShellExec;

import com.teradata.fnc.AMPInfo;
import com.teradata.fnc.NodeInfo;
import com.teradata.fnc.Tbl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.BlockLocation;
//import org.apache.hadoop.record.compiler.generated.ParseException;

import java.io.*;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.lang.Object;
import java.util.*;
import java.math.BigDecimal;

class GenCtx implements Serializable
{
	public int id;   //the index to the cache element which contains the FSDataInputStream used by this AMP.
	public long startpos; // the first byte in the DFS file to be read by this AMP
	public long currentpos;  //the next byte in the DFS file to be read by this AMP in the next round of building a database row
	private long DfsBytesCnt;   //the number of rows should be read from DFS by this AMP
	private long DfsBytesRead = 0; //the number of rows have been read from DFS by this AMP
    private List<String> type;

	public GenCtx()
	{
	}

	/**
	 * 
	 * @param id
	 *            the index to the cache element which contains the FSDataInputStream used by this AMP.
	 * @param startpos
	 *            the first byte in the DFS file to be read by this AMP
	 * @param DfsRowsCnt
	 *            the number of rows to be retreived by this AMP
	 * @rowsize   the size of the row 
	 * @throws  
	 */

	public GenCtx(int id, long startpos, long DfsBytesCnt, List<String> type)
	{
		this.id = id;
		this.startpos = startpos;
		this.DfsBytesCnt = DfsBytesCnt;
        this.type = type;
		currentpos = startpos;


	}
	
	/**Create a database row from reading a line in the DFS file
	 * 
	 * 
	 * @param in
	 *             the FSDataInputStream used by the AMP.
	 * @param c1
	 *            the array containing the first column vlaue to be retured to DBS
	 * @param c2
	 *            the array containing the second column vlaue to be retured to DBS
	 * @rowsize    
	 * @throws  IOException
	 */

	public int CreateRow(FSDataInputStream in, String delim, Object[] ...args) throws IOException
	{
		if (DfsBytesRead == DfsBytesCnt) return 0; // No more rows; This AMP has loaded all rows assigned to it.
		in.seek(currentpos);
		BufferedReader bufferIn = new BufferedReader(new InputStreamReader(in));
		String line;

		//read a line from the DFS
		line = bufferIn.readLine();

		//parse the two integers in the line read
		String[] yb = line.split("\\\001");
		
        int ybLen = yb.length;
        //Iterator<String> it = type.iterator();
        for(int i = 0; i < ybLen; i++)
        {
            String str = type.get(i);
            args[i][0] = typeCast(yb[i],str);
        }
		currentpos += (line.length()+1);
		DfsBytesRead += (line.length()+1);
		return 1;
	}

    private Object typeCast(String value, String type)
    {
        if (type.equals("int")) {
            int temp = Integer.parseInt(value.trim());
            return temp;
        } else if (type.equals("byte")) {
        	byte[] b = value.getBytes();
        	return b;
            
        } else if (type.equals("date")) {
        	Date date = null;
        	try {
        		DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        		date = format.parse(value);
        	} catch (ParseException e) {
        		e.printStackTrace();
        	}
        	
        	return date;
        } else if (type.equals("BigDecimal")) {
        	BigDecimal bd = new BigDecimal(value);
        	return bd;
        } else if (type.equals("long")) {
        	long l = Long.parseLong(value);
        	return l;
        } else if (type.equals("double")) {
        	Double d = Double.parseDouble(value);
        	return d;
        } else if (type.equals("float")) {
        	Float f = Float.parseFloat(value);
        	return f;
        } else if (type.equals("boolean")) {
        	Boolean b = Boolean.parseBoolean(value);
        	return b;
        }

        return value;
    }
}

public class HDFS_UDF {
	private static int last_id = 0;   //the last used cell in the cache array
	private static final int max_ids = 1000;
	private static String tableName = null;
	private static String destinationLocation = null;
	

	//The array keeps the list of FSDataInput Stream opened by all AMPs to access HDFS. Each AMP uses a FSDataInputStream to access HDFS.
	private static final FSDataInputStream[] cache = new FSDataInputStream[max_ids];
	
	public static void GetDFSFileData(String filename, String xmlname, String hdfsname,  String delim, String whereClause,
            Object[] c1,
            Object[] c2,
            Object[] c3,
            Object[] c4,
            Object[] c5,
            Object[] c6,
            Object[] c7,
            Object[] c8,
            Object[] c9,
            Object[] c10
            /*String[] c11,
            String[] c12,
            String[] c13,
            String[] c14,
            String[] c15,
            String[] c16,
            String[] c17,
            String[] c18,
            String[] c19,
            String[] c20,
            String[] c21,
            String[] c22,
            String[] c23,
            String[] c24,
            String[] c25*/)
			throws SQLException, IOException, ClassNotFoundException, Exception
	{
		int status;
		int[] phase = new int[1];
		GenCtx obj;
		Tbl tbl = new Tbl();



		/* make sure the function is called in the supported context */
		switch (tbl.getPhase(phase))
		{
			case Tbl.TBL_MODE_CONST:
				/* depending on the phase decide what to do */
				switch (phase[0])
				{
					case Tbl.TBL_PRE_INIT:

						//HDFS related setup
						Configuration conf = new Configuration();
						conf.setClassLoader(Configuration.class.getClassLoader());

						conf.set("fs.default.name", hdfsname);
						conf.set("user", "root");
		                FileSystem fs = FileSystem.get(conf);


						Path inFile = new Path(filename);

						// Check if input is valid
						if (!fs.exists(inFile))
							throw new IOException(filename + "  does not exist");
						if (!fs.isFile(inFile))
							throw new IOException(filename + " is not a file");
						return;

					case Tbl.TBL_INIT:
						/* get scratch memory to keep track of things */

						// Create ID for this particular SQL+AMP instance. 
						int id = getNewId();
						
						// set up the information needed to build a row by this AMP 
						obj = InitializeGenCtx(filename, xmlname, hdfsname, id, whereClause);

						//store the GenCtx object created which will be used to create the first row
						tbl.allocCtx(obj);
						tbl.setCtxObject(obj);

						break;
					case Tbl.TBL_BUILD:

						// Get the GenCtx from the scratch pad from the last time.
						obj = (GenCtx)tbl.getCtxObject();
						int myid = obj.id;
						
						status = obj.CreateRow(cache[myid], delim, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10);
								 //c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25*/);

						if (status == 0)
							throw new SQLException("no more data", "02000");

						tbl.setCtxObject(obj);
						break;

					case Tbl.TBL_END:
						int my_id = ((GenCtx)tbl.getCtxObject()).id;
						cache[my_id].close();
						cache[my_id] = null;

						break;
				}
				return;

			case Tbl.TBL_MODE_VARY:
				throw new SQLException("Table VARY mode is not supported.");

		}

	}
	
	private static GenCtx InitializeGenCtx(String filename, String xmlname, String hdfsname, int id, String whereClause) throws IOException, SQLException, Exception
	{
		//connection DFS
		Configuration conf = new Configuration();
		conf.setClassLoader(Configuration.class.getClassLoader());
		conf.set("fs.default.name", hdfsname);
		conf.set("user", "root");
		FileSystem fs = FileSystem.get(conf);
		
		//parse the table definition xml file
		DomParseXML dom = new DomParseXML();
        Path xml = new Path(xmlname);
        FSDataInputStream xmlFile = fs.open(xml);
        dom.parseXMLData(xmlFile);
        
        //create Hive table
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd HHmmss");
        //List<String> nameList = dom.getNameList();
        List<String> nameList = new ArrayList<String>();
        nameList.add("userid");
        nameList.add("movieid");
        //List<String> typelist = dom.getTypeList();
        List<String> typelist = new ArrayList<String>();
        typelist.add("int");
        typelist.add("int");
        if (tableName == null) {
        	tableName = "tmp_tbl_" + formatter.format(new Date()).replace(" ", "_");
        }
        if (destinationLocation == null) {
        	 destinationLocation = "/tmp/tmp_file_" + formatter.format(new Date()).replace(" ", "_");
        }
        //String fileLocation = hdfsname + filename;
        //String recordDelimiter = dom.getRecordDelimiter();
        String recordDelimiter = "//\n";
        //String fieldDelimiter = dom.getFieldDelimiter();
        String fieldDelimiter = "|";
        String createTableSQL = createTableSQL(nameList, typelist, tableName, recordDelimiter, fieldDelimiter);
        String dataSQL = loadDataSQL(tableName,filename);
        String querySQL = createQuerySQL(tableName, whereClause, destinationLocation);
        String dropTableSQL = dropTableSQL(tableName);
        
        String absolute_filename = filename.substring(filename.lastIndexOf("/")+1);
        String absolute_dictory = filename.substring(0,filename.lastIndexOf("/"));
		
		synchronized(RemoteShellExec.class) {
			RemoteShellExec rse = new RemoteShellExec("192.168.146.205","jason","111111");
			if (RemoteShellExec.count == 0) {
				int ret = rse.execHiveSQL("set mapred.reduce.tasks = 1;");
				if (ret != 0) {
					System.exit(ret);
				}
				
				ret = rse.execHiveSQL(createTableSQL);
				if (ret != 0) {
					System.exit(ret);
				}
				
				ret = rse.execHiveSQL(dataSQL);
				if (ret != 0) {
					System.exit(ret);
				}
				
				ret = rse.execHiveSQL(querySQL);
				if (ret != 0) {
					System.exit(ret);
				}
				
				ret = rse.exec("/usr/local/hadoop/bin/hadoop fs -mv /user/hive/warehouse/" + tableName + "/" + absolute_filename + " " + absolute_dictory);
				if (ret != 0) {
					System.exit(ret);
				}
				
				ret = rse.execHiveSQL(dropTableSQL);
				if (ret != 0) {
					System.exit(ret);
				}
				
			}
		}
		
		Path inFile = new Path(destinationLocation + "/000000_0");
		FileStatus fstatus = fs.getFileStatus(inFile);
		long len = fstatus.getLen();
		BlockLocation[] blks = fs.getFileBlockLocations(fstatus, 0,len);
		int blkCount = blks.length;
		

		FSDataInputStream in = fs.open(inFile);


		/* get the number of AMPs, compute the AMP id of this AMP
		 * The N-th AMP reads the N-th portion of the DFS file
		 */

		AMPInfo amp_info = new AMPInfo();
		NodeInfo node_info = new NodeInfo();

		int[] amp_ids = node_info.getAMPIds();
		int ampcnt = node_info.getNumAMPs(); // the number of AMPs in the Teradata system
		int amp_id = amp_info.getAMPId();  //the id of this AMP

		//long size = fstatus.getLen();       //the size of the DFS file
		//long totalRowsCnt = size / ROWSIZE;  // the total number of lines in the DFS file

		// some "heavy" AMPs will read one more line than other "light" AMPs. avgRowCnt is the number of lines "light" AMPs will read.
		int avgBlks = blkCount / ampcnt;

		int heavyBlksAMPId = (int)(blkCount % ampcnt); // the id of the first "heavy" AMP 

		//long myRowsCnt2Read = avgRowCnt;  //how many rows this AMP should load
		long[] bytesCntAMP = new long[ampcnt];  // this array records how many rows each AMP should load
		
		for (int k = 0; k < heavyBlksAMPId; k++)
		{
			int i;
			if(k == 0)
			{
				i = k * avgBlks;
			}
			else
			{
				i = k * avgBlks + 1;
			}
			for(; i < (k+1)*avgBlks+1; i++)
			{
				bytesCntAMP[k] += blks[i].getLength();
			}
		}
		for (int k = heavyBlksAMPId; k < ampcnt; k++)
		{
			for (int i = k*avgBlks; i < (k+1)*avgBlks; i++)
			{
				bytesCntAMP[k] += blks[i].getLength();
			}
		}

		long bytesCntBeforeMe = 0; //total number of DFS lines (counting from the begining of the DFS file) other AMPs before this AMP will read, i.e., the number of DFS lines this AMP should skip
		for (int k = 0; k < ampcnt; k++)
		{
			if (amp_id == amp_ids[k])
			{
				break;
			}
			else
				bytesCntBeforeMe += bytesCntAMP[k];


		}
		
		long startpos = bytesCntBeforeMe; //the first byte in the DFS file this AMP should read.
	
		cache[id] = in;
 
        long newpos = 0;
        long oldpos = 0;
        if (amp_id == ampcnt - 1) {
        	RemoteShellExec.count = 0;
        }
       if (amp_id == 0) {
        	in.seek(bytesCntBeforeMe+bytesCntAMP[amp_id]);
        	BufferedReader br = new BufferedReader(new InputStreamReader(in));
        	String str = br.readLine();
        	if(str != null) {
        		newpos = str.length() + 1;
        	}
        	return new GenCtx(id, startpos, bytesCntAMP[amp_id]+newpos,typelist);
        } else if (amp_id == ampcnt-1) {
        	in.seek(bytesCntBeforeMe);
        	BufferedReader br = new BufferedReader(new InputStreamReader(in));
        	String newstr = br.readLine();
        	if (newstr != null) {
        		oldpos = newstr.length() + 1;
        	}
        	return new GenCtx(id, startpos+oldpos, bytesCntAMP[amp_id]-oldpos,typelist);
        } else {
        	in.seek(bytesCntBeforeMe+bytesCntAMP[amp_id]);
        	BufferedReader newbr = new BufferedReader(new InputStreamReader(in));
        	String str = newbr.readLine();
        	if (str != null) {
        		newpos = str.length() + 1;
        	}
        	
        	in.seek(bytesCntBeforeMe);
        	BufferedReader oldbr = new BufferedReader(new InputStreamReader(in));
        	String oldstr = oldbr.readLine();
        	if (oldstr != null) {
        		oldpos = oldstr.length() + 1;
        	}
        	return new GenCtx(id, startpos+oldpos, bytesCntAMP[amp_id]-oldpos+newpos,typelist);
        }
		//return new GenCtx(id, startpos, bytesCntAMP[amp_id],list);
//       ret = rse.exec("/usr/local/hadoop/bin/hadoop fs -rmr " + destinationLocation);
//		if (ret != 0) {
//			System.exit(ret);
//		}
	}
	
	private static String createTableSQL(List<String> namelist, List<String> typelist, String tablename, 
			String recorddelimiter, String fielddelimiter) {
		String tableSQL = "CREATE TABLE IF NOT EXISTS" + " " + tablename + " " + "(";
		int len = namelist.size();
		for (int i = 0; i < len; ++i) {
			tableSQL += "\n";
			if (i == len-1) {
				tableSQL += namelist.get(i) + " " + typelist.get(i) + ")";
			} else {
				tableSQL += namelist.get(i) + " " + typelist.get(i) + ",";
			}
		}
		tableSQL += "\n";
		tableSQL += "ROW FORMAT DELIMITED";
		tableSQL += "\n";
		tableSQL += "  " + "FIELDS TERMINATED BY " + "\'" +fielddelimiter + "\'" + "\n";
		//tableSQL += "  " + "LINES TERMINATED BY " + "\'" + recorddelimiter + "\'" + "\n";
		tableSQL += "STORED AS TEXTFILE" + ";";
		return tableSQL;
		
	}
	
	private static String loadDataSQL(String tablename, String filelocation) {
		String dataSQL = "LOAD DATA INPATH " + "\'" + filelocation + "\'" + " INTO TABLE " + tablename + ";";
		
		return dataSQL;
	}
	
	private static String createQuerySQL(String tablename, String whereClause, String destnationLocation) {
		String querySQL = "INSERT OVERWRITE DIRECTORY ";
		querySQL += "\'" + destnationLocation + "\'";
		querySQL += " " + "SELECT * FROM " + tablename;
		querySQL += " " + whereClause + ";";
		
		return querySQL;
	}
	
	private static String dropTableSQL(String tablename) {
		String dropTableSQL = "DROP TABLE IF EXISTS " + tablename + ";";
		
		return dropTableSQL;
	}
	
	private synchronized static int getNewId()
	{
		last_id = (last_id + 1) % max_ids;
		return last_id;
	}
}
