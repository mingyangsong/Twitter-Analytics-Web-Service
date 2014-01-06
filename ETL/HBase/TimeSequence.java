package cloud_computing;

import java.io.IOException;
import java.text.DateFormat;
import java.text.Normalizer;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.commons.lang.StringEscapeUtils;


public class TimeSequence {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
      private Text key = new Text();
      private Text value = new Text();
      
      public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String line = value.toString().trim();
        JSONParser parser = new JSONParser();
        
          
        Date date = new Date();
        try {
			JSONObject obj = (JSONObject)parser.parse(line);
			String time = (String)obj.get("created_at");
			DateFormat form1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ y");
			DateFormat form2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
            date = null;
			try {
				date = sdf.parse(time);
			} catch (java.text.ParseException e) { }
            
			String dateStr;
			if (date != null) dateStr = form2.format(date);
			else dateStr = "unknown date";
			
			String tweetId = (String)obj.get("id_str");
            
			Pattern pattern = Pattern.compile("\"text\":\"(.*?[^\\\\])\",");
			Matcher match = pattern.matcher(line);
			String text = new String();
			if(match.find()){
				text = match.group(1);
			}
			
			key.set(dateStr);
			value.set(tweetId+":"+text);

			output.collect(key,value);
			
		} catch (java.lang.Exception e) { }
      }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
          
        ArrayList<String> tweets = new ArrayList<String>();
        while (values.hasNext()) { tweets.add(values.next().toString().trim()); }
        
        TweetComparator compartor = new TweetComparator();
        Collections.sort(tweets, compartor);
        StringBuilder sortedTweets = new StringBuilder();
          
        for( int i = 0;i < tweets.size();i++){
            String current = tweets.get(i);
        	if ( i == 0 || compartor.compare(tweets.get(i), tweets.get(i-1)) == 1 ) {
                sortedTweets.append(current);
                sortedTweets.append('\n');
            }
        }
        output.collect(key, new Text(sortedTweets.toString()));
      }
    }

    public static class TweetComparator implements Comparator<String> {
        @Override
        public int compare(String o1, String o2) {
        	String id1 = o1.split(":", 2)[0];
        	String id2 = o2.split(":", 2)[0];
        	id1  = ("0000000000000000000" + id1).substring(id1.length());
        	id2  = ("0000000000000000000" + id2).substring(id2.length());
            return id1.compareTo(id2);
        }
    }
    
    public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(TimeSequence.class);
      
      conf.setJobName("timesequence");

      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(Text.class);

      conf.setMapperClass(Map.class);
      conf.setReducerClass(Reduce.class);

      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(SequenceFileOutputFormat.class);

      FileInputFormat.setInputPaths(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));

      JobClient.runJob(conf);
    }
}

