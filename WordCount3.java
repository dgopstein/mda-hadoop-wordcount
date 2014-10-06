import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class WordCount3 {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    HashMap<String, Integer> hm = new HashMap<String, Integer>();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {
            String nextWord = tokenizer.nextToken();

            if (nextWord.length() == 7) {
                if (!hm.containsKey(nextWord)) hm.put(nextWord, 0);

                hm.put(nextWord, hm.get(nextWord) + 1);
            }
        }

        Iterator it = hm.entrySet().iterator();
        for (int i = 0; i < 100 && it.hasNext(); i++) {
            Entry pairs = (Entry)it.next();
            context.write(new Text((String)pairs.getKey()), new IntWritable((Integer)pairs.getValue()));
        }
    }
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    HashMap<String, Integer> hm = new HashMap<String, Integer>();
    TreeMap<String,Integer> sortedMap;

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        hm.put(key.toString(), sum);
        WordCount3ValueComparator vc =  new WordCount3ValueComparator(hm);
        sortedMap = new TreeMap<String,Integer>(vc);
        sortedMap.putAll(hm);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator it = sortedMap.entrySet().iterator();
        for (int i = 0; i < 100; i++) {
            Entry pairs = (Entry)it.next();
            context.write(new Text((String)pairs.getKey()), new IntWritable((Integer)pairs.getValue()));
        }
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "wordcount");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(WordCount3.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}

// http://www.programcreek.com/2013/03/java-sort-map-by-value/
class WordCount3ValueComparator implements Comparator<String> {
    Map<String, Integer> map;

    public WordCount3ValueComparator(Map<String, Integer> base) {
        this.map = base;
    }

    public int compare(String a, String b) {
        if (map.get(a) >= map.get(b)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys 
    }
}
