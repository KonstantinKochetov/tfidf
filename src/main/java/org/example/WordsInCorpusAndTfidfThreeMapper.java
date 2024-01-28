package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordsInCorpusAndTfidfThreeMapper extends Mapper<LongWritable, Text, Text, Text> {

    public WordsInCorpusAndTfidfThreeMapper() {
    }

    /**
     * @param key is the byte offset of the current line in the file;
     * @param value is the line from the file
     *
     *     Input: marcello@book.txt  \t  3/1500
     *     Output: marcello, book.txt=3/1500
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] wordAndCounters = value.toString().split("\t");
        String[] wordAndDoc = wordAndCounters[0].split("@");                 //3/1500
        context.write(new Text(wordAndDoc[0]), new Text(wordAndDoc[1] + "=" + wordAndCounters[1]));
    }
}

