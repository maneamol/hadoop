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

/**
 * This is an example Hadoop Map/Reduce application.
 * It reads the text input files, breaks each line into words
 * and counts them. The output is a locally sorted list of words and the 
 * count of how often they occurred.
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar wordcount
 *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 */
public class WordCount extends Configured implements Tool {
  
  /**
   * Counts the words in each line.
   * For each line of input, break the line into words and emit them as
   * (<b>word</b>, <b>1</b>).
   */
  public static class MapClass extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, IntWritable> {
    
    // array of preposition words which added to set
    private final static String[] prepositionWords = { "aboard", "about",
        "above", "across", "after", "against", "along", "amid", "among",
        "anti", "around", "as", "at", "before", "behind", "below",
        "beneath", "beside", "besides", "between", "beyond", "but", "by",
        "concerning", "considering", "despite", "down", "during", "except",
        "excepting", "excluding", "following", "for", "from", "in",
        "inside", "into", "like", "minus", "near", "of", "off", "on",
        "onto", "opposite", "outside", "over", "past", "per", "plus",
        "regarding", "round", "save", "since", "than", "through", "to",
        "toward", "towards", "under", "underneath", "unlike", "until",
        "up", "upon", "versus", "via", "with", "within", "without" };
    private final static Set<String> prepositionSet = new HashSet<String>(Arrays.asList(prepositionWords));

    // array of common words which added to set
    private final static String[] mostUsedWords = { "i", "it", "for", "not",
        "on", "with", "he", "as", "you", "do", "at", "this", "but", "his",
        "by", "from", "they", "we", "say", "her", "she", "or", "an",
        "will", "my", "one", "all", "would", "there", "their", "what",
        "so", "up", "out", "if", "about", "who", "get", "which", "go",
        "me", "when", "make", "can", "like", "time", "no", "just", "him",
        "know", "take", "person", "into", "year", "your", "good", "some",
        "could", "them", "see", "other", "than", "then", "now", "look",
        "only", "come", "its", "over", "think", "also", "back", "after",
        "use", "two", "how", "our", "work", "first", "well", "way", "even",
        "new", "want", "because", "any", "these", "give", "day", "most",
        "us", "the", "be", "to", "of", "and", "a", "in", "that", "have" };
    private final static Set<String> mostUsedSet = new HashSet<String>(Arrays.asList(mostUsedWords));


    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    public void map(LongWritable key, Text value, 
                    OutputCollector<Text, IntWritable> output, 
                    Reporter reporter) throws IOException {
      String line = value.toString();
      StringTokenizer itr = new StringTokenizer(line);
      int count = 1;
      String temp = null;
      while (itr.hasMoreTokens()) {
        if (count ==1) {
          itr.nextToken();
          count++;
        }
        else {
          temp = itr.nextToken().toLowerCase().trim();
          if(!"".equals(temp)) {
            if (temp.contains("mulligan")) {
              word.set("mulligan");
              output.collect(word, one);
            }
            if (temp.contains("buck")) {
              word.set("buck");
              output.collect(word, one);
            }
            temp = temp.replaceAll("[^a-zA-Z ]", "").trim();
            if((!prepositionSet.contains(temp)) && (!mostUsedSet.contains(temp)) && (!"".equals(temp)))  {
              word.set("total");
              output.collect(word, one);
              //word.set(temp);
              //output.collect(word, one);
            }
          }
        }
      }
    }
  }
  
  /**
   * A reducer class that just emits the sum of the input values.
   */
  public static class Reduce extends MapReduceBase
    implements Reducer<Text, IntWritable, Text, IntWritable> {
    
    public void reduce(Text key, Iterator<IntWritable> values,
                       OutputCollector<Text, IntWritable> output, 
                       Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }
  
  static int printUsage() {
    System.out.println("wordcount [-m <maps>] [-r <reduces>] <input> <output>");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }
  
  /**
   * The main driver for word count map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the 
   *                     job tracker.
   */
  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), WordCount.class);
    conf.setJobName("wordcount");
 
    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    conf.setOutputValueClass(IntWritable.class);
    
    conf.setMapperClass(MapClass.class);        
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
    
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
    int res = ToolRunner.run(new Configuration(), new WordCount(), args);
    System.exit(res);
  }

}
