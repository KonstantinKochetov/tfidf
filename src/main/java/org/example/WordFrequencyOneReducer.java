package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordFrequencyOneReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public WordFrequencyOneReducer() {
    }

    /**
     * @param key is the key of the mapper
     * @param values are all the values aggregated during the mapping phase
     * @param context contains the context of the job run
     *
     *      Input: receive a list of <"word@filename",[1, 1, 1, ...]> pairs
     *        <"marcello@a.txt", [1, 1]>
     *
     *      Output: emit the output a single key-value where the sum of the occurrences.
     *        <"marcello@a.txt", 2>
     */
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        //write the key and the adjusted value (removing the last comma)
        context.write(key, new IntWritable(sum));
    }
}
