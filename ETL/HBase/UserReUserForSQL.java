package cloud_computing;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

public class UserReUserForSQL {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
      private Text key = new Text();
      private Text value = new Text();
      
      public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
          
        String line = value.toString().trim();
        JSONParser parser = new JSONParser();
    
        try {
            JSONObject obj = (JSONObject)parser.parse(line);
            HashMap tweetFrom = (HashMap)obj.get("retweeted_status");
            if(tweetFrom != null){
                HashMap fromUser = (HashMap)tweetFrom.get("user");
                HashMap user = (HashMap)obj.get("user");
                
                key.set( (String)from_user.get("id_str") );
                value.set( (String)user.get("id_str") );
                  
                output.collect(key, value);
            }
        } catch (java.lang.Exception e) { }
      }
    }

    public static class TweetCompare implements Comparator<String> {
        @Override
        public int compare(String o1, String o2) {
        	o1  = ("0000000000000000000" + o1).substring(o1.length());
        	o2  = ("0000000000000000000" + o2).substring(o2.length());
            return o1.compareTo(o2);
        }
    }
    
    
    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        ArrayList<String> tweets = new ArrayList<String>();
        
        while (values.hasNext()) { tweets.add(values.next().toString()); }
          
        TweetComparator tweetcomparator = new TweetComparator();
        
        Collections.sort(tweets, tweetcompare);
        StringBuilder sortedTweets = new StringBuilder();
        
        for( int i = 0;i < tweets.size();i++){
            String current = tweets.get(i);
            if ( i == 0 || compartor.compare(tweets.get(i), tweets.get(i-1)) > 0 ) {
                sortedTweets.append(current);
                sortedTweets.append('\n');
            }
        }
        String value = sortedTweets.toString().replace("\n", "\\\\X0A");
        output.collect(key, new Text(value));
      }
    }

    public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(TabGnerateUserReUser.class);
      
      conf.setJobName("UserReUser");

      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(Text.class);

      conf.setMapperClass(Map.class);
      conf.setReducerClass(Reduce.class);

      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      FileInputFormat.setInputPaths(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));

      JobClient.runJob(conf);
    }
}

