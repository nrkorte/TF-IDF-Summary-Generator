package classes;

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
import java.util.PriorityQueue;
import java.util.Arrays;
import java.util.Collections;
import java.util.AbstractMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;


import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

public class ProfileB extends Configured implements Tool {

    public static class ProfileBMapperOne extends Mapper<Object, Text, Text, Text> {

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
            Double tfidf = Double.parseDouble(values.get(1));

            context.write(new Text("A"+docId), new Text(unigram + " " + tfidf.toString()));
        }
    }


    public static class ProfileBMapperTwo extends Mapper<Object, Text, Text, Text> {

        public String formatDoc(String docText) {
            String ret = docText.replaceAll("[\\'\"()$&,*{}\\t\\];0-9%]", "");
            ret = ret.replaceAll("[^\\x00-\\x7F]", "").toLowerCase();
            return ret;
        }
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String rawText = value.toString();

            String[] parts = rawText.split("<====>");
            String docId = parts[1].trim();
            String docBody = formatDoc(parts[2]);

            String[] sentences = docBody.split("[.!?]");

            List<String> sentenceList = new ArrayList<>(Arrays.asList(sentences));
            for (int i = 0; i < sentences.length; i++) {
                sentences[i] = sentences[i].trim().replaceAll("[.?!]", "");
            }

            for (String sentence : sentenceList) {
                context.write(new Text("B" + docId), new Text(sentence));
            }
        }
    }


    public static class ProfileBReducer extends Reducer<Text, Text, Text, Text> {

        private Map<String, Double> unigramTFIDF = new HashMap<>();
        private Map<String, Double> top5Sentences = new HashMap<>();
        private List<String> allSentences = new ArrayList<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String keyString = key.toString();

            if (keyString.startsWith("A")) {
                processUnigramTFIDF(values);
            } else if (keyString.startsWith("B")) {
                addSentences(values);
            }
        }

        private void processUnigramTFIDF(Iterable<Text> values) {
            for (Text value : values) {
                String[] parts = value.toString().split(" ");
                String unigram = parts[0];
                Double tfidf = Double.parseDouble(parts[1]);
                unigramTFIDF.put(unigram, tfidf);
            }
        }

        private void addSentences(Iterable<Text> values) {
            for (Text value : values) {
                String sentence = value.toString();
                allSentences.add(sentence);
            }
        }

        private void processSentences(String sentence) {
            double cumulativeTFIDF = calculateSentenceTFIDF(sentence);

            if (top5Sentences.size() < 5) {
                top5Sentences.put(sentence, cumulativeTFIDF);
            } else {
                double lowestScore = Double.MAX_VALUE;
                String lowestSentence = null;
                for (Map.Entry<String, Double> entry : top5Sentences.entrySet()) {
                    if (entry.getValue() < lowestScore) {
                        lowestScore = entry.getValue();
                        lowestSentence = entry.getKey();
                    }
                }
                if (cumulativeTFIDF > lowestScore) {
                    top5Sentences.remove(lowestSentence);
                    top5Sentences.put(sentence, cumulativeTFIDF);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String sentence : allSentences) {
                processSentences(sentence);
            }
            int i = 0;
            for (Map.Entry<String, Double> entry : top5Sentences.entrySet()) {
                i++;
                context.write(new Text("Sentence " + i + ": " + entry.getKey() + " ==> "), new Text(Double.toString(entry.getValue()) + "\n"));
            }
        }

        private double calculateSentenceTFIDF(String sentence) {
            String[] words = sentence.split("\\s+");

            List<Double> sentenceTFIDFValues = new ArrayList<>();

            for (String word : words) {
                Double tfidf = unigramTFIDF.get(word.toLowerCase().replaceAll("[.,!?]", ""));
                if (tfidf != null) {
                    sentenceTFIDFValues.add(tfidf);
                } else {
                    System.err.println("TF-IDF value not found for word: " + word);
                }
            }

            // Sort the TF-IDF values in descending order
            Collections.sort(sentenceTFIDFValues, Collections.reverseOrder());

            // Take the top 5 values (or all values if less than 5)
            int topN = Math.min(5, sentenceTFIDFValues.size());
            double cumulativeTFIDF = 0.0;
            for (int i = 0; i < topN; i++) {
                cumulativeTFIDF += sentenceTFIDFValues.get(i);
            }


            return cumulativeTFIDF;
        }

        // @Override
        // protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //     // Iterate through the values and print them
        //     for (Text value : values) {
        //         context.write(key, value);
        //     }
        // }
    }




    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int resB = runJob(conf, args[0], args[1]);
        return resB;
    }

    public static int runJob(Configuration conf, String inputDir, String outputDir) throws Exception {
        /*
        Profile B
        Job 1:
            - Generate Summary for any given article
            - Split by a period and a single space ". " to fetch sentences
            - Then only preprocess unigrams (remove non-alphanumeric characters)
            - You need to have multiple inputs to this job. You need the output of job 3 in job A and you need the given article
                - You can do this one of two ways:
                - Use MultipleInputs.addInputPath() and join them on keys (reduce side join) <- this is easier
                - or you can persist output from pervious job in Hadoop's DistributedCache
        */

        Configuration confB = new Configuration();
        // confB.set("mapreduce.framework.name", "yarn");
        Job profileB = Job.getInstance(confB, "Profile B");
        profileB.setJarByClass(ProfileB.class);
        profileB.setNumReduceTasks(1);
        profileB.setReducerClass(ProfileBReducer.class);

        profileB.setOutputKeyClass(Text.class);
        profileB.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(profileB, new Path(inputDir + "_job2"), TextInputFormat.class, ProfileBMapperOne.class);
        MultipleInputs.addInputPath(profileB, new Path(inputDir + "_input"), TextInputFormat.class, ProfileBMapperTwo.class);
        FileOutputFormat.setOutputPath(profileB, new Path(outputDir + "_profileB"));
        return profileB.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        try {
            int exitCode = ToolRunner.run(new ProfileB(), args);
            System.exit(exitCode);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

// hadoop jar ~/PA2/profile_b/ProfileB.jar classes.ProfileB -D mapreduce.framework.name=yarn /PA2/output /PA2/profile_b_out

// hadoop jar ~/PA2/profile_b/ProfileB.jar classes.ProfileB /PA2/output /PA2/profile_b_out