package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WordFrequencyOneMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public WordFrequencyOneMapper() {
    }


    /**
     * @param key is the byte offset of the current line in the file;
     * @param value is the line from the file
     *
     *  Output <"word", "filename@offset"> pairs
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Regex for finding words
        Matcher m = Pattern.compile("\\w+").matcher(value.toString());

        // Get the name of the file
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
//        String languageName = ((FileSplit) context.getInputSplit()).getPath().getParent().getName();

        // iterate over each word
        while (m.find()) {
            StringBuilder valueBuilder = new StringBuilder();

            String matchedKey = m.group().toLowerCase();
            // remove names starting with non letters, digits, other chars
            if (!Character.isLetter(matchedKey.charAt(0)) || Character.isDigit(matchedKey.charAt(0)) || matchedKey.contains("_")) {
                continue;
            }
            valueBuilder.append(matchedKey);
            valueBuilder.append("@");
            valueBuilder.append(fileName);
//            valueBuilder.append("#");
//            valueBuilder.append(languageName);
            // output
            context.write(new Text(valueBuilder.toString()), new IntWritable(1));
            valueBuilder.delete(0, valueBuilder.length());
        }
    }
}
