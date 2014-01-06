package cloud;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
 
public class UserReUserBulkLoadPrepare {
	private static boolean debug = false;
    public static class Map extends Mapper<Text, Text, ImmutableBytesWritable, KeyValue> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            try {
            	if(debug)
            		System.out.println("key:"+key.toString()+"~~~~value:"+value.toString());
            	ImmutableBytesWritable hkey = new ImmutableBytesWritable();
            	hkey.set(Bytes.toBytes(key.toString()));
            	KeyValue hvalue = new KeyValue(Bytes.toBytes(key.toString()),Bytes.toBytes("Users"),Bytes.toBytes("all"),System.currentTimeMillis(),Bytes.toBytes(value.toString()));
    			context.write(hkey,hvalue);
    			
    		} catch (java.lang.Exception e) {
    			//do nothing but continue
    		}
          }

    } 
 
    
 
    public static void main(String[] args) throws Exception {
    	 Configuration conf = new Configuration();
    	 if(!debug){
	         conf.set("hbase.table.name", "UserReUsers");
	         HBaseConfiguration.addHbaseResources(conf);
    	 }
         
         Job job = new Job(conf, "WordCount");
         job.setJarByClass(UserReUserBulkLoadPrepare.class); 
         
         job.setMapperClass(Map.class);
         job.setMapOutputKeyClass(ImmutableBytesWritable.class);
         job.setMapOutputValueClass(KeyValue.class);
  
         job.setInputFormatClass(SequenceFileAsTextInputFormat .class);   
         
         if(!debug){
        	 String tableName = "UserReUsers"; 
             HTable htable = new HTable(conf, tableName); 
             HFileOutputFormat.configureIncrementalLoad(job, htable); 
         }
         FileInputFormat.addInputPath(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job, new Path(args[1]));
  
         job.waitForCompletion(true);


    }        
}