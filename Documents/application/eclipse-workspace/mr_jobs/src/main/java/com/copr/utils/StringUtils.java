package com.copr.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {
	public static Boolean isDigt(String val) {
		Pattern p = Pattern.compile("^(\\d+)$");
		Matcher m = p.matcher(val);
		if(m.find())
			return true;
		else
			return false;
	}
	public static Boolean isLetter(String val) {
		Pattern p = Pattern.compile("^[a-zA-Z]+$");
		Matcher m = p.matcher(val);
		if(m.find())
			return true;
		else
			return false;
	}
	public static Boolean isOther(String val) {
		Pattern p = Pattern.compile("^[a-zA-Z]+$");
		Matcher m = p.matcher(val);
		if(m.find())
			return true;
		else
			return false;
	}
	
	public static void main(String[] args) {
		System.out.println("/123".split("/").length);
		String str = "asdfasdfAfdf1";
		System.out.println(isLetter(str));
	}
}
