import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//import java.io.BufferedReader;
import java.io.IOException;
//import java.io.InputStreamReader;
import java.lang.Integer;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

// Don't Change >>>
public class TopOntimeArrival extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(TopOntimeArrival.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopOntimeArrival(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/T1/G1Q2/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Arrival Count");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(ArrivalCountMap.class);
        jobA.setReducerClass(ArrivalCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopOntimeArrival.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Ontime Arrival");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(TopOntimeArrivalMap.class);
        jobB.setReducerClass(TopOntimeArrivalReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopOntimeArrival.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }


    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }

    public static class ArrivalCountMap extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Pattern p = Pattern.compile("\\d+");
            Matcher m = p.matcher(line);
            int i = 0;
            int delay = 0; 
			int ontime = 0;
			String airlineId = null;

            while (m.find()) {
                i++;
                if (i == 1)
                    airlineId = m.group();
                else if (i == 2) {
                    delay = Integer.parseInt(m.group());
				    if (delay <= 0) {
						ontime = 1;
					}
				}
    		}
            //System.out.println("ArrivalCountMap: " + airlineId + " " + ontime); 
            //System.out.format("ArrivalCountMap '%s', '%d'\n", airlineId, ontime);
			//LOG.info("VIC-ArrivalCountMap:"+airlineId+"  "+ ontime);
            // write only when both key and value are available, discard otherwise
            if ((airlineId != null) && (i == 2)) 
                context.write(new Text(airlineId), new IntWritable(ontime));      
            
        }
    }

    public static class ArrivalCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int total = 0;
            int perf = 0;
            int ontime = 0;
	        for (IntWritable val : values) {
			    total++;
                if (val.get() == 0 ) {
                    ontime++;
                }
                
	    	}
			//LOG.info("VIC-ArrivalCountReduce:"+total+"  "+ontime);
            perf = 100 * ontime / total;   // use integer to keep simple
            //System.out.println("ArrivalCountReduce: "+perf+"  "+total+"  "+ontime); 
            
	    context.write(key, new IntWritable(perf));            
        }
    }

    public static class TopOntimeArrivalMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
        Integer N;
        private TreeSet<Pair<Integer, String>> topN = new TreeSet<Pair<Integer, String>>();
        
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Integer count = Integer.parseInt(value.toString());
            String word = key.toString();
  
            topN.add(new Pair(count, word));
                    
            if (topN.size() > N) {
                topN.remove(topN.first());
            }                 
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Pair<Integer, String> item : topN) {
                String[] strings = {item.second, item.first.toString()};
                TextArrayWritable val = new TextArrayWritable(strings);
                context.write(NullWritable.get(), val);
            }                          
        }
    }

    public static class TopOntimeArrivalReduce extends Reducer<NullWritable, TextArrayWritable, Text, IntWritable> {
        Integer N;
        private TreeSet<Pair<Integer, String>> topN = new TreeSet<Pair<Integer, String>>();
        
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {

            for (TextArrayWritable value : values) {
                Text[] textArray = (Text[]) value.toArray();

                String airline = textArray[0].toString();
                Integer perf = Integer.parseInt(textArray[1].toString());
                //System.out.println("TopOntimeArrivalReduce: " + airline + " " + perf); 
                topN.add(new Pair<Integer, String>(perf, airline));

                if (topN.size() > N) {
                    topN.remove(topN.first());
                }
            }
            
            for (Pair<Integer, String> item: topN) {
                Text airline = new Text(item.second);
                IntWritable perfcount = new IntWritable(item.first);
                context.write(airline, perfcount);
            }
        }
    }

}

// >>> Don't Change
class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
// <<< Don't Change
