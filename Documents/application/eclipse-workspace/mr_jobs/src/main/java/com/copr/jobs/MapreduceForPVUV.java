package com.copr.jobs;
import java.io.IOException;

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

/**
 * Created by chc 
 */
public class MapreduceForPVUV extends Configured implements Tool {

    static class myMapperPUV extends Mapper<LongWritable,Text,Text,IntWritable>{
        IntWritable one = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strings = value.toString().split(",");
            context.write(new Text(strings[0]+":"+strings[1]),one);
            context.write(new Text(strings[0]+":"+strings[1]+":"+strings[2]),one);
        }
    }
    static class myReducerPUV extends Reducer<Text ,IntWritable,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value:values){
                sum+=value.get();
            }
            context.write(new Text(key.toString()+":"+sum),NullWritable.get());
        }
    }

    static class myMapperPV extends Mapper<LongWritable,Text,Text,IntWritable >{
        IntWritable one = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
           String[] strings = values.toString().split(":");
            if(strings.length==4){
                context.write(new Text(strings[0]+":"+strings[1]+":uv"),one);
            }else if(strings.length==3){
                context.write(new Text(strings[0]+":"+strings[1]),new IntWritable(Integer.parseInt(strings[2])));
            }
        }
    }
    static class myReducerPV extends Reducer<Text,IntWritable,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value:values){
                sum+=value.get();
            }
            context.write(new Text(key.toString()+":"+sum),NullWritable.get());
        }
    }
    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new MapreduceForPVUV(), args);
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
        job.setJarByClass(MapreduceForPVUV.class);
        job.setMapperClass(MapreduceForPVUV.myMapperPUV.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MapreduceForPVUV.myReducerPUV.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass( TextInputFormat.class );
        job.setOutputFormatClass( TextOutputFormat.class );

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath( job,new Path(args[1]));
        if(job.waitForCompletion(true)){
            Job jobN = new Job(conf,"mapreduceN");
            Path mypaths = new Path(args[2]);
            FileSystem hdfss = mypaths.getFileSystem(conf);
            if (hdfss.isDirectory(mypaths)) {
                hdfss.delete(mypaths, true);
            }
            jobN.setJarByClass(MapreduceForPVUV.class);
            jobN.setMapperClass(myMapperPV.class);
            jobN.setMapOutputKeyClass(Text.class);
            jobN.setMapOutputValueClass(IntWritable.class);

            jobN.setReducerClass(myReducerPV.class);
            jobN.setOutputKeyClass(Text.class);
            jobN.setOutputValueClass(NullWritable.class);

            jobN.setInputFormatClass( TextInputFormat.class );
            jobN.setOutputFormatClass( TextOutputFormat.class );

            FileInputFormat.addInputPath(jobN, new Path(args[1]));
            FileOutputFormat.setOutputPath( jobN,new Path(args[2]) );
            
            return jobN.waitForCompletion(true)?0:1;
        }else{
        	return -1;
        }
        	
        	
	}
}