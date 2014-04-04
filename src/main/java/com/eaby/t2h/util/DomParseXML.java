package com.ebay.t2h.util;

import org.apache.hadoop.fs.FSDataInputStream;
import org.dom4j.io.SAXReader;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

public class DomParseXML {
	public DomParseXML() {
		nameList = new ArrayList<String>();
		typeList = new ArrayList<String>();
		sourceTypeList = new ArrayList<String>();
		nullableList = new ArrayList<String>();
	}
	
	public void parseXMLData(FSDataInputStream in) {
		SAXReader reader = new SAXReader();
	
		try {
			//File file = new File();
			Document doc = reader.read(in);
			
			Element root = doc.getRootElement();
			Element table = root.element("table");
			Element storage = table.element("storage");
			recordDelimiter = storage.attributeValue("recordDelimiter");
			Element value = table.element("value");
			fieldDelimiter = value.attributeValue("fieldDelimiter");
			
			Iterator it = value.elementIterator();
			String str = null;
			while(it.hasNext()) {
				Element field = (Element) it.next();
				//get column name
				str = field.attributeValue("name");
				nameList.add(str);
				
				//get column type
				str = field.attributeValue("type");
				typeList.add(str);
				
				//get column sourceType
				str = field.attributeValue("sourceType");
				sourceTypeList.add(str);
				
				//column is null?
				str = field.attributeValue("isNullable");
				nullableList.add(str);
			}
		} catch (DocumentException e) {
			e.printStackTrace();
		}
	}
	
	public List<String> getNameList() {
		return nameList;
	}
	
	public List<String> getTypeList() {
		return typeList;
	}
	
	public List<String> getsourceTypeList() {
		return sourceTypeList;
	}
	
	public List<String> getNullableList() {
		return nullableList;
	}
	
	public String getFileFormat() {
		return fileFormat;
	}
	
	public String getRecordDelimiter() {
		return recordDelimiter;
	}
	
	public String getFieldDelimiter() {
		return fieldDelimiter;
	}
	
	private List<String> nameList;
	private List<String> typeList;
	private List<String> sourceTypeList;
	private List<String> nullableList;
	private String recordDelimiter;
	private String fieldDelimiter;
	private String fileFormat;
	
}
