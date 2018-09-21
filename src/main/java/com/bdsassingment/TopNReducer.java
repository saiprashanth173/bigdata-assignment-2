package com.bdsassingment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

import static com.bdsassingment.Defaults.DEFAULT_LIMIT;

public class TopNReducer extends Reducer<NullWritable, Text, Text, IntWritable> {
    private final static TreeSet<Pair> topN = new TreeSet<>();

    private int top;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration config = context.getConfiguration();
        top = config.getInt("top", DEFAULT_LIMIT);
    }

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] pairTuple = value.toString().split("\t");
            Pair pair = new Pair(pairTuple[0], Integer.parseInt(pairTuple[1]));
            topN.add(pair);
            if (topN.size() > top) {
                topN.pollFirst();
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        while (!topN.isEmpty()) {
            Pair pair = topN.pollFirst();
            context.write(new Text(pair.getWord()), new IntWritable(pair.getCount()));
        }
    }


}
