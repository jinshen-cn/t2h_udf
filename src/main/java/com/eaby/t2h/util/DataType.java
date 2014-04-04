package com.ebay.t2h.util;

public class DataType {

	public static final byte UNKNOWN = 0;
	public static final byte NULL = 1;
	public static final byte BOOLEAN = 5;
	public static final byte BYTE = 6;
	public static final byte INTERGE = 10;
	public static final byte LONG = 15;
	public static final byte FLOAT = 20;
	public static final byte DOUBLE = 25;
	public static final byte DATE = 30;
	public static final byte BIGDECIMAL = 35;
	public static final byte STRING = 40;
	
	public static byte findType(String type) {
		if (type.equals("boolean")) {
			return BOOLEAN;
		} else if (type.equals("byte")) {
			return BYTE;
		} else if (type.equals("int")) {
			return INTERGE;
		} else if (type.equals("long")) {
			return LONG;
		} else if (type.equals("float")) {
			return FLOAT;
		} else if (type.equals("double")) {
			return DOUBLE;
		} else if (type.equals("date")) {
			return DATE;
		} else if (type.equals("BigDecimal")) {
			return BIGDECIMAL;
		} else {
			return STRING;
		}
	}
	
}
