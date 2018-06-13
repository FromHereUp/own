package com.copr.test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class RegexTest {
	public static void main(String[] args) {
		String str = "教师10[0010] (320)";
		
		//String reg = ".*\\((.*)\\).*";
		Pattern r = Pattern.compile("(?<=\\[)(?<field>00.+?)(?=\\])");
		Matcher m = r.matcher(str);
		while(m.find()) {
			System.out.println(m.group("id"));
		}
				
	}
}
