package cloud_computing;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.apache.hadoop.io.MapFile;

public class UserCountForSQL {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private Text key = new Text();
        private Text value = new Text();
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            
            String line = value.toString().trim();
            JSONParser parser = new JSONParser();
            
            try{
                JSONObject obj = (JSONObject)parser.parse(line);
                HashMap user = (HashMap)obj.get("user");
                String user_id = (String)usr.get("id_str");
                String tw_id = (String)obj.get("id_str");
                String normalized_id  = ("0000000000000000000" + user_id).substring(user_id.length());
                
                // set the key and value
                key.set(normalized_id);
                value.set(tw_id);
                output.collect(key, value);
            }
            catch(java.lang.Exception e){ }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, IntWritable> {
      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        HashSet<String> tweets = new HashSet<String>();
        while (values.hasNext()) {
          tweets.add(values.next().toString());
        }
        output.collect(key, new IntWritable(tweets.size()));
      }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(UserCount.class);
        conf.setJobName("userCount");
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        
        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
        JobClient.runJob(conf);
    }
}

