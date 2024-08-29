package com.assignment1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private static final Pattern pattern = Pattern.compile("^[ACGWY][a-z]+$");

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s+");

        for (String token : tokens) {
            if (pattern.matcher(token).matches()) {
                word.set(token);
                context.write(word, one);
            }
        }
    }
}
