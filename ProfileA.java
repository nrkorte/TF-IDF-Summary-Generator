package patwo;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;



public class ProfileA extends Configured implements Tool {

    public static enum Counters {
        TOTAL_DOCS
    }

    public static class JobOneMapper extends Mapper<Object, Text, Text, MapWritable> {
        
        /* Gets the unigrams, maps them and pairs them with a temporary frequency of 1
           Adds every new documents that we see to the TOTAL_DOCS counter
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String rawText = value.toString();

            Doc doc = new Doc(rawText);
            Map<String, Doc.DocInfo> documentMap = doc.getDocumentMap();

            context.getCounter(ProfileA.Counters.TOTAL_DOCS).increment(documentMap.size());

            for (Map.Entry<String, Doc.DocInfo> var : documentMap.entrySet()) {
                String docId = var.getKey();
                Doc.DocInfo docInfo = var.getValue();
                String docText = doc.formatDoc(docInfo.getDocumentBody());

                StringTokenizer itr = new StringTokenizer(docText);

                while (itr.hasMoreTokens()) {
                    String unigram = itr.nextToken();

                    MapWritable m = new MapWritable();
                    m.put(new Text(unigram), new IntWritable(1));
                    // m.put(new Text(Integer.toString(documentMap.size())), new IntWritable(1));
                    m.put(new Text("maxFrequency"), new IntWritable(docInfo.getMaxFrequency()));

                    context.write(new Text(docId), m);
                }
            }
        }
    }


    public static class JobOneReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
        /* Compiles all unigrams and their frequencies together
           Calculates the ni_value for each unigram
         */
        private HashMap<Text, Set<Text>> unigramToDocIds = new HashMap<>();

        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable result = new MapWritable();
            IntWritable maxFrequency = null;

            for (MapWritable value : values) {
                for (Writable k : value.keySet()) {
                    Text unigram = (Text) k;
                    IntWritable frequency = (IntWritable) value.get(k);

                    if (result.containsKey(unigram)) {
                        IntWritable freq = (IntWritable) result.get(unigram);
                        freq.set(freq.get() + frequency.get());
                    } else {
                        result.put(unigram, frequency);
                    }

                    if (!unigramToDocIds.containsKey(unigram)) {
                        unigramToDocIds.put(unigram, new HashSet<>());
                    }
                    Text docID = (Text) key;
                    unigramToDocIds.get(unigram).add(docID);

                    maxFrequency = (IntWritable) value.get(new Text("maxFrequency"));
                }
            }


            for (Writable keyWritable : result.keySet()) {
                Text unigram = (Text) keyWritable;
                int ni_value = unigramToDocIds.get(unigram).size();

                MapWritable outputMap = new MapWritable();
                outputMap.put(new Text("unigram"), unigram);
                outputMap.put(new Text("unigram_frequency"), (IntWritable) result.get(unigram));
                outputMap.put(new Text("ni_value"), new IntWritable(ni_value));
                outputMap.put(new Text("max_frequency"), maxFrequency);

                context.write(key, outputMap);
            }
        }
    }


    public static class JobTwoMapper extends Mapper<Object, Text, Text, MapWritable> {
        /* Grabs output from last job, gets the local frequencies and max frequency of the unigram in the document and emits them */
        private Map<String, Integer> documentMaxFrequencies = new HashMap<>();

        public static List<String> create_list(String input) {
            List<String> values = new ArrayList<>();

            input = input.replace("{", "").replace("}", "");

            String[] keyValuePairs = input.split(",\\s+");
            for (String keyValuePair : keyValuePairs) {
                String[] keyValue = keyValuePair.split("=");
                String value;
                if (keyValue.length != 2) 
                    value = keyValue[0].trim();
                else
                    value = keyValue[1].trim();
                values.add(value);
            }

            return values;
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String content = value.toString();

            String[] parts = content.split("\\s+", 2);

            List<String> values = create_list(parts[1]);
            String docId = parts[0];

            String unigram = values.get(0);
            int freq = Integer.parseInt(values.get(1));
            int ni_value = Integer.parseInt(values.get(2));

            int max = Integer.parseInt(values.get(3));

            MapWritable m = new MapWritable();
            m.put(new Text("unigram"), new Text(unigram));
            m.put(new Text("unigram_frequency"), new IntWritable(freq));
            m.put(new Text("ni_value"), new IntWritable(ni_value));
            m.put(new Text("max_frequency"), new IntWritable(max));
            

            context.write(new Text(docId), m);
        }
    }


    public static class JobTwoReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
        private long N;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            N = context.getConfiguration().getLong(ProfileA.Counters.TOTAL_DOCS.name(), 0);
        }
        /* 
        Calculates the tf value for each unigram
         */
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

            for (MapWritable value : values) {
                Text unigram = (Text) value.get(new Text("unigram"));
                IntWritable frequency = (IntWritable) value.get(new Text("unigram_frequency"));
                IntWritable maxFreq = (IntWritable) value.get(new Text("max_frequency"));
                IntWritable ni_value = (IntWritable) value.get(new Text("ni_value"));

                double t1= (double)frequency.get();
                double t2= (double)maxFreq.get();
                Double tf = 0.5 + (0.5 * (t1 / t2));
                Double idf = Math.log10((double) N / (double) ni_value.get());
                Double tfidf = tf * idf;
                // if (tfidf > 15) {
                //     System.err.println("ERROR: OUTLIER DETECTED =====> " + unigram + ":" + tfidf);
                // }


                MapWritable result = new MapWritable();
                result.put(new Text("unigram"), unigram);
                result.put(new Text("TF-IDFValue"), new DoubleWritable(tfidf));

                context.write(key, result);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ProfileA(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("err: needs 2 arguments. found " + (args.length - 1));
            return 1;
        }
        Configuration conf = new Configuration();
        int resA = runJob(conf, args[0], args[1]);
        return resA;
    }

    public static int runJob(Configuration conf, String inputDir, String outputDir) throws Exception {
        // Profile A
        /*
        // Job 1: Remove non-alphanumeric characters from documents
           find freq of unigrams in each document
           output[reducer] = <docID, freq>
        */
        Configuration conf1 = new Configuration();
        // conf1.set("mapreduce.framework.name", "yarn");
        Job job1 = Job.getInstance(conf1, "Job 1");
        job1.setNumReduceTasks(1);
        job1.setJarByClass(ProfileA.class);
        job1.setMapperClass(JobOneMapper.class);
        job1.setReducerClass(JobOneReducer.class);
        job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(MapWritable.class);
        FileInputFormat.addInputPath(job1, new Path(inputDir));
        FileOutputFormat.setOutputPath(job1, new Path(outputDir + "_job1"));
        job1.waitForCompletion(true);

        Counter counter = job1.getCounters().findCounter(Counters.TOTAL_DOCS);
        long totalDocs = counter.getValue();
        /*
        // [Formula] TFij = 0.5+0.5(fij/max(k)fkj), where (f(ij)/max(k)f(kj)) is the maximum raw frequency of any term k in in the article j
        // Job 2: Caluclate the TF of each unigram using formula above.
           In mapper, loop through all frequencies and find the highest one and that is going to be the value for f(ij)/max(k)f(kj)
           Mapper output = <docID, <unigram, frequency>>
           !in reducer
           iterate through all the values and find the max(k)fkj
           calculate the TF for each unigram
           output <docID, unigram TFvalue>
        */

        Configuration conf2 = new Configuration();
        // conf2.set("mapreduce.framework.name", "yarn");
        Job job2 = Job.getInstance(conf2, "Job 2");
        job2.setNumReduceTasks(1);
        job2.setJarByClass(ProfileA.class);
        job2.setMapperClass(JobTwoMapper.class);
        job2.setReducerClass(JobTwoReducer.class);

        job2.setOutputKeyClass(Text.class); // DOC_ID
        job2.setOutputValueClass(MapWritable.class); // <unigram, TFValue>

        job2.getConfiguration().setLong(counter.getDisplayName(), counter.getValue());

        FileInputFormat.addInputPath(job2, new Path(outputDir + "_job1"));
        FileOutputFormat.setOutputPath(job2, new Path(outputDir + "_job2"));
        return job2.waitForCompletion(true) ? 0 : 1;
    }
}

// hadoop jar ~/PA2/ProfileA.jar patwo.ProfileA -D mapreduce.framework.name=yarn /PA2/input /PA2/output

// hadoop jar ~/PA2/ProfileA.jar patwo.ProfileA /PA2/input /PA2/output