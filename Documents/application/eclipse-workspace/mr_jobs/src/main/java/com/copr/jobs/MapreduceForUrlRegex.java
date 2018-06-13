package com.copr.jobs;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.copr.utils.StringUtils;


/**
 * Created by chc 
 */
public class MapreduceForUrlRegex extends Configured implements Tool {

    static class myMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        //sid,sname,cid,cname,scid,scname,host,regex,path,url,matcherType,key,type,option,opt
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strings = new String(value.toString().getBytes(),"gb2312").split(",");
            context.write(new Text(strings[0]+"-"+strings[2]+"-"+strings[4]+"-"+strings[6]),value);
        }
    }
    static class myReducer extends Reducer<Text ,Text,Text,NullWritable>{
    	static Map<String,Set<Map<String,String>>> regexs = new HashMap<String,Set<Map<String,String>>>();
    	
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value:values){
            	String vals[] = value.toString().split(",");
            	String path = vals[8];
            	if(regexs.containsKey(key)) {
            		Iterator<Map<String,String>> iter = regexs.get(key.toString()).iterator();
            		boolean flag = false;
            		while(iter.hasNext()) {
            			Pattern p = Pattern.compile(iter.next().get("regrex"));
            			if(p.matcher(path).find()) {
            				flag = true;
            				break;
            			}
            		}
            		if(!flag) {
            			String regrexStr = getRegex(path);
            			Map<String,String> map = new HashMap<String,String>();
            			map.put("sid", vals[0]);
            			map.put("sname", vals[1]);
            			map.put("cid", vals[2]);
            			map.put("cname", vals[3]);
            			map.put("scid", vals[4]);
            			map.put("scname", vals[5]);
            			map.put("host", vals[6]);
            			map.put("path", vals[8]);
            			map.put("regex", regrexStr);
            			regexs.get(key.toString()).add(map);
            		}
            	}else {
            		String regrexStr = getRegex(path);
        			Map<String,String> map = new HashMap<String,String>();
        			map.put("sid", vals[0]);
        			map.put("sname", vals[1]);
        			map.put("cid", vals[2]);
        			map.put("cname", vals[3]);
        			map.put("scid", vals[4]);
        			map.put("scname", vals[5]);
        			map.put("host", vals[6]);
        			map.put("path", vals[8]);
        			map.put("regex", regrexStr);
        			regexs.put(key.toString(), new HashSet<Map<String,String>>());
        			regexs.get(key.toString()).add(map);
            	}
                
            }
        }
        @Override
        protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
        		throws IOException, InterruptedException {
        	
        	for(String key:regexs.keySet()) {
        		Iterator<Map<String,String>> iter = regexs.get(key).iterator();
        		while(iter.hasNext()) {
        			StringBuffer val = new StringBuffer();
        			Map<String,String> map = iter.next();
        			val.append(map.get("sid")).append(",");
        			val.append(map.get("sname")).append(",");
        			val.append(map.get("cid")).append(",");
        			val.append(map.get("cname")).append(",");
        			val.append(map.get("scid")).append(",");
        			val.append(map.get("scname")).append(",");
        			val.append(map.get("host")).append(",");
        			val.append(map.get("path")).append(",");
        			val.append(map.get("regex"));
        			
        			context.write(new Text(val.toString()), NullWritable.get());
        		}
        	}
        	
        }
        public String getRegex(String value) {
        	StringBuffer sb = new StringBuffer();
        	String pathArr[] = value.split("/");
        	if(pathArr.length==0) {
        		sb.append("^$");
        		return sb.toString();
        	}else {
        		if(value.startsWith("/")) {
        			sb.append("^/");
        			for(int i=1;i<pathArr.length;i++) {
            			if(StringUtils.isDigt(pathArr[i])) {
            				sb.append("(\\d+)");
            			}else if(StringUtils.isLetter(pathArr[i])) {
            				sb.append("([a-zA-Z]+)");
            			}else {
            				if(pathArr[i].indexOf(".")>0) {
            					String tmpA[] = pathArr[i].split("\\.");
            					String tmp = tmpA[0];
            					if(tmp.indexOf("-")>0) {
            						int ii=0;
            						for(String v :tmp.split("-")) {
            							if(StringUtils.isDigt(v)) {
            	            				sb.append("(\\d+)");
            	            			}else if(StringUtils.isLetter(v)) {
            	            				sb.append("([a-zA-Z]+)");
            	            			}else {
            	            				sb.append("(\\w+)");
            	            			}
            							if(ii<tmp.split("-").length-1)
            								sb.append("-");
            							ii++;
            						}
            					}else if(tmp.indexOf("_")>0) {
            						int jj=0;
            						for(String v :tmp.split("_")) {
            							if(StringUtils.isDigt(v)) {
            	            				sb.append("(\\d+)");
            	            			}else if(StringUtils.isLetter(v)) {
            	            				sb.append("([a-zA-Z]+)");
            	            			}else {
            	            				sb.append("(\\w+)");
            	            			}
            							if(jj<tmp.split("_").length-1)
            								sb.append("_");
            							jj++;
            						}
            					}else {
            						if(StringUtils.isDigt(tmp)) {
        	            				sb.append("(\\d+)");
        	            			}else if(StringUtils.isLetter(tmp)) {
        	            				sb.append("([a-zA-Z]+)");
        	            			}else {
        	            				sb.append("(\\w+)");
        	            			}
            					}
            					sb.append("\\.").append(tmpA[1]);
            				}
            			}
            			if(i<pathArr.length-1)
            				sb.append("/");
            		}
        			sb.append("$");
        		}
        	}
        	
        	
        	return sb.toString();
        }
    }

    
    public static void main(String[] args) throws Exception {
    	System.setProperty("hadoop.home.dir","D:\\hadoop-2.7.5" );
        int res = ToolRunner.run(new Configuration(), new MapreduceForUrlRegex(), args);
        System.exit(res);

    }
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf,"mapreduce");

        Path mypath = new Path(args[1]);
        FileSystem hdfs = mypath.getFileSystem(conf);
        if (hdfs.isDirectory(mypath)) {
            hdfs.delete(mypath, true);
        }
        job.setJarByClass(MapreduceForUrlRegex.class);
        job.setMapperClass(myMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(myReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass( TextInputFormat.class );
        job.setOutputFormatClass( TextOutputFormat.class );

        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath( job,new Path(args[1]));
        return job.waitForCompletion(true)?0:1;
        	
	}
}