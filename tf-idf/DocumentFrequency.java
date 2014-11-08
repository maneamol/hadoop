package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DocumentFrequency extends Configured implements Tool {
  public static class MapClass extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, IntWritable> {
    // Variables
    private final static IntWritable one = new IntWritable(1);
    
    private Text wordSher = new Text();
    

    private Text wordBuck = new Text();
    

    public void map(LongWritable key, Text value, 
                    OutputCollector<Text, IntWritable> output,
                    Reporter reporter) throws IOException {
      //remove non string characters from string and convert it to lowercase
      String line = value.toString();
      int count = 1;
      int linenumber = 0;

      wordSher.set("mulligan");
      wordBuck.set("buck");


      if (line != null) {
        StringTokenizer itr = new StringTokenizer(line);
        line = value.toString().replaceAll("[^a-zA-Z ]", "").toLowerCase();
        if (itr.hasMoreTokens() && count ==1) {
          
          linenumber = Integer.parseInt(itr.nextToken().trim());
          linenumber = linenumber / 35;
          System.out.println(linenumber);
          count++;
        }
        // ckeck for strings where sherlock and holmes is in same line
        if (line.contains("mulligan")) {
          
          output.collect(wordSher, new IntWritable(linenumber));
        }
        // ckeck for strings where buck and mulligam is in same line
        if (line.contains("buck")) {
 
          output.collect(wordBuck, new IntWritable(linenumber));
        }
      }
    }
  }
  
  public static class ReduceClass extends MapReduceBase
    implements Reducer<Text, IntWritable, Text, IntWritable> {
    
    public void reduce(Text key, Iterator<IntWritable> values,
                       OutputCollector<Text, IntWritable> output, 
                       Reporter reporter) throws IOException {
      HashSet<Integer> uniqueDocIds = new HashSet<Integer>();
      int temp = 0;
      while (values.hasNext()) {
        temp = values.next().get();
        uniqueDocIds.add(temp);
      }
      output.collect(key, new IntWritable(uniqueDocIds.size()));
    }
  }
  
  static int printUsage() {
    System.out.println("DocumentFrequency [-m <maps>] [-r <reduces>] <input> <output>");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }
  
  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), DocumentFrequency.class);
    conf.setJobName("DocumentFrequency");
 
    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    conf.setOutputValueClass(IntWritable.class);
    
    conf.setMapperClass(MapClass.class);        
    //conf.setCombinerClass(ReduceClass.class);
    conf.setReducerClass(ReduceClass.class);
    
    List<String> other_args = new ArrayList<String>();
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-m".equals(args[i])) {
          conf.setNumMapTasks(Integer.parseInt(args[++i]));
        } else if ("-r".equals(args[i])) {
          conf.setNumReduceTasks(Integer.parseInt(args[++i]));
        } else {
          other_args.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " +
                           args[i-1]);
        return printUsage();
      }
    }
    // Make sure there are exactly 2 parameters left.
    if (other_args.size() != 2) {
      System.out.println("ERROR: Wrong number of parameters: " +
                         other_args.size() + " instead of 2.");
      return printUsage();
    }
    FileInputFormat.setInputPaths(conf, other_args.get(0));
    FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
        
    JobClient.runJob(conf);
    return 0;
  }
  
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new DocumentFrequency(), args);
    System.exit(res);
  }
}
