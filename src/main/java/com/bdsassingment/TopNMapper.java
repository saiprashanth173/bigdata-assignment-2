package com.bdsassingment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Objects;
import java.util.TreeSet;

import static com.bdsassingment.Defaults.DEFAULT_LIMIT;
import static java.lang.Integer.parseInt;

public class TopNMapper
        extends Mapper<LongWritable, Text, NullWritable, Text> {


    private final static TreeSet<Pair> topN = new TreeSet<>();
    private int top;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration config = context.getConfiguration();
        top = config.getInt("top", DEFAULT_LIMIT);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString();

        String columns[] = row.split("\t");
        String word = columns[0];
        int count = parseInt(columns[1]);
        topN.add(new Pair(word, count));
        if (topN.size() > top) {
            topN.pollFirst();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        while (!topN.isEmpty()) {
            Pair pair = topN.pollFirst();
            context.write(NullWritable.get(), new Text(Objects.requireNonNull(pair).concatinated()));
        }
    }
}