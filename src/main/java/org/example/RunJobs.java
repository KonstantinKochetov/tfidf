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
import org.example.*;

import java.util.HashMap;

public class RunJobs extends Configured implements Tool {

    String BASE_INPUT_PATH = "/tfidf/data/input/";
    String BASE_OUTPUT_PATH = "/tfidf/data/output/";

    public int run(String[] args) throws Exception {

        Configuration config = getConf();
        FileSystem fileSystem = FileSystem.get(config);

        HashMap<String, HashMap<String, String>> languagePaths = new HashMap<>();

        // Processing each file in the input directory
        RemoteIterator<LocatedFileStatus> fileIterator = fileSystem.listFiles(new Path(BASE_INPUT_PATH), true);
        while (fileIterator.hasNext()) {
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

        for (String currentLanguage : languagePaths.keySet()) {
            config = getConf();
            HashMap<String, String> currentLanguageMap = languagePaths.get(currentLanguage);

            Job job1 = executeJob(1, config, currentLanguage, currentLanguageMap.get("INPUT"), currentLanguageMap.get("OUTPUT_JOB_1"),
                    WordFrequencyOneMapper.class, WordFrequencyOneReducer.class, Text.class, IntWritable.class);
            job1.waitForCompletion(true);

            Job job2 = executeJob(2, config, currentLanguage, currentLanguageMap.get("OUTPUT_JOB_1"), currentLanguageMap.get("OUTPUT_JOB_2"),
                    WordCountsTwoMapper.class, WordCountsTwoReducer.class, Text.class, Text.class);
            job2.waitForCompletion(true);

            Job job3 = executeJob(3, config, currentLanguage, currentLanguageMap.get("OUTPUT_JOB_2"), currentLanguageMap.get("OUTPUT_JOB_3"),
                    WordsInCorpusAndTfidfThreeMapper.class, WordsInCorpusAndTfidfThreeReducer.class, Text.class, Text.class);

            Path inputPath = new Path(currentLanguageMap.get("INPUT"));
            FileSystem fs_2 = inputPath.getFileSystem(config);
            FileStatus[] stat = fs_2.listStatus(inputPath);
            job3.setJobName(String.valueOf(stat.length));
            job3.waitForCompletion(true);
        }

        return 0;
    }


    private Job executeJob(int idx, Configuration config, String language, String inputPath,
                            String outputPath, Class mapperClass, Class reducerClass,
                            Class<?> outputKeyClass, Class<?> outputValueClass) throws Exception {
        Job job = new Job(config, language + " Processing " + "input: " + inputPath);
        job.setJarByClass(RunJobs.class);
        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);
        if (idx == 1) job.setCombinerClass(reducerClass);
        job.setOutputKeyClass(outputKeyClass);
        job.setOutputValueClass(outputValueClass);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new org.example.RunJobs(), args);
        System.exit(res);
    }
}
