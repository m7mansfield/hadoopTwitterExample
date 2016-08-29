import java.io.IOException;
import java.lang.String;
import java.io.StringReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;

public class Twitter {

  public static class RetweetMapper
  extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException {
      String line = new String(value.toString());
      JsonReader reader = Json.createReader(new StringReader(line));
      JsonObject tweetJson = reader.readObject();
      reader.close();
      JsonObject retweetJson = tweetJson.getJsonObject("retweeted_status");
      if(retweetJson != null) {
        JsonObject userJson = retweetJson.getJsonObject("user");
        String screenName = userJson.getString("screen_name");
        Text resScreenName = new Text(screenName);
        IntWritable resRetweetCount = new IntWritable(1);
        context.write(resScreenName, resRetweetCount);
      }
    }
  }

  public static class RetweetCombiner 
  extends Reducer<Text,IntWritable,Text,IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values,
    Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      IntWritable result = new IntWritable(sum);
      context.write(key, result);
    }
  }

  public static class RetweetReducer 
  extends Reducer<Text,IntWritable,Text,IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values,
    Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      int bound = Integer.parseInt(conf.get("bound"));
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      if (sum > bound) {
        IntWritable result = new IntWritable(sum);
        context.write(key, result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("bound", args[0]);
    Job job = Job.getInstance(conf, "twitter");
    job.setJarByClass(Twitter.class);
    job.setMapperClass(RetweetMapper.class);
    job.setCombinerClass(RetweetCombiner.class);
    job.setReducerClass(RetweetReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
