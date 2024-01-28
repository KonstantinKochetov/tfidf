package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import org.apache.hadoop.fs.Path;

import java.util.HashMap;

public class RunJobs extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        String BASE_INPUT_PATH = "/tfidf/data/input/";
        String BASE_OUTPUT_PATH = "/tfidf/data/output/";

        Configuration config = getConf();
        FileSystem fileSystem = FileSystem.get(config);

        HashMap<String, HashMap<String, String>> languagePaths = new HashMap<>();

        // Processing each file in the input directory
        RemoteIterator<LocatedFileStatus> fileIterator = fileSystem.listFiles(new Path(BASE_INPUT_PATH), true);
        while(fileIterator.hasNext()){
            LocatedFileStatus fileStatus = fileIterator.next();
            String language = fileStatus.getPath().getParent().getName();

            // Constructing file paths for each processing job
            HashMap<String, String> filePaths = new HashMap<>();
            filePaths.put("INPUT", BASE_INPUT_PATH + language);
            filePaths.put("OUTPUT_JOB_1", BASE_OUTPUT_PATH + language + "/job1");
            filePaths.put("OUTPUT_JOB_2", BASE_OUTPUT_PATH + language + "/job2");
            filePaths.put("OUTPUT_JOB_3", BASE_OUTPUT_PATH + language + "/job3");

            languagePaths.put(language, filePaths);
        }

        // Executing jobs for each language
        for (String language : languagePaths.keySet()) {
            HashMap<String, String> paths = languagePaths.get(language);

            executeJob(config, language, paths.get("INPUT"), paths.get("OUTPUT_JOB_1"),
                    WordFrequencyOneMapper.class, WordFrequencyOneReducer.class);
            executeJob(config, language, paths.get("OUTPUT_JOB_1"), paths.get("OUTPUT_JOB_2"),
                    WordCountsTwoMapper.class, WordCountsTwoReducer.class);
            executeJob(config, language, paths.get("OUTPUT_JOB_2"), paths.get("OUTPUT_JOB_3"),
                    WordsInCorpusAndTfidfThreeMapper.class, WordsInCorpusAndTfidfThreeReducer.class);
        }

        return 0;
    }

    private void executeJob(Configuration config, String language, String inputPath,
                            String outputPath, Class mapperClass, Class reducerClass) throws Exception {
        Job job = Job.getInstance(config, language + " Processing");
        job.setJarByClass(RunJobs.class);
        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RunJobs(), args);
        System.exit(res);
    }
}
